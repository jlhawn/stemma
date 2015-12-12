package stemma

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// RemoteObjectStore represents a connection to a remote object store.
type RemoteObjectStore interface {
	GetTag(name string) (Descriptor, error)
	ListTags() (map[string]Descriptor, error)
	Fetch(desc Descriptor, progress *ProgressMeter) error
	Push(desc Descriptor, progress *ProgressMeter) error
}

type remoteObjectStore struct {
	r       *Repository
	baseURL *url.URL
}

func (r *Repository) RemoteObjectStore(remoteURL string) (RemoteObjectStore, error) {
	parsed, err := url.Parse(remoteURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse remote URL: %s", err)
	}

	switch parsed.Scheme {
	case "http", "https":
		// These schemes are currently supported.
	default:
		return nil, fmt.Errorf("unspported scheme: %q", parsed.Scheme)
	}

	return &remoteObjectStore{
		r:       r,
		baseURL: parsed,
	}, nil
}

func (ros *remoteObjectStore) GetTag(name string) (Descriptor, error) {
	query := url.Values{}
	query.Set("service", "get-tag")
	query.Set("tag", name)

	reqURL := new(url.URL)
	*reqURL = *ros.baseURL
	reqURL.RawQuery = query.Encode()

	resp, err := http.Get(reqURL.String())
	if err != nil {
		return nil, fmt.Errorf("unable to make get-tag request to remote: %s", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNoSuchTag
	}

	desc, err := UnmarshalDescriptor(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to decode descriptor from response: %s", err)
	}

	return desc, nil
}

func (r *Repository) HandleGetTag(rw http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	tag := req.Form.Get("tag")

	desc, err := r.TagStore().Get(tag)
	if err != nil {
		if err == ErrNoSuchTag {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		log.Printf("unable to get descriptor for tag %q: %s", tag, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := MarshalDescriptor(rw, desc); err != nil {
		log.Printf("unable to encode descriptor for tag %q: %s", tag, err)
	}
}

func (ros *remoteObjectStore) ListTags() (map[string]Descriptor, error) {
	query := url.Values{}
	query.Set("service", "list-tags")

	reqURL := new(url.URL)
	*reqURL = *ros.baseURL
	reqURL.RawQuery = query.Encode()

	resp, err := http.Get(reqURL.String())
	if err != nil {
		return nil, fmt.Errorf("unable to make list-tags request to remote: %s", err)
	}

	defer resp.Body.Close()

	tagDescriptors, err := UnmarshalTagDescriptors(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to decode tag descriptors: %s", err)
	}

	return tagDescriptors, nil
}

func (r *Repository) HandleListTags(rw http.ResponseWriter, req *http.Request) {
	tags, err := r.TagStore().List()
	if err != nil {
		log.Printf("unable to list tags: %s", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	tagDescriptors := make(map[string]Descriptor, len(tags))

	for _, tag := range tags {
		desc, err := r.TagStore().Get(tag)
		if err != nil {
			log.Printf("unable to get descriptor for tag %q: %s", tag, err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		tagDescriptors[tag] = desc
	}

	if err := MarshalTagDescriptors(rw, tagDescriptors); err != nil {
		log.Printf("unable to encode tag descriptors: %s", err)
	}
}

func (ros *remoteObjectStore) newConn() (conn net.Conn, err error) {
	switch ros.baseURL.Scheme {
	case "http":
		conn, err = net.Dial("tcp", ros.baseURL.Host)
	case "https":
		conn, err = tls.Dial("tcp", ros.baseURL.Host, nil)
	default:
		err = fmt.Errorf("unsupported transport scheme: %q", ros.baseURL.Scheme)
	}

	if err != nil {
		return nil, fmt.Errorf("unable to get remote connection: %s", err)
	}

	return conn, nil
}

func (ros *remoteObjectStore) Fetch(desc Descriptor, progress *ProgressMeter) error {
	query := url.Values{}
	query.Set("service", "serve-objects")

	reqURL := new(url.URL)
	*reqURL = *ros.baseURL
	reqURL.RawQuery = query.Encode()

	req, err := http.NewRequest("POST", reqURL.String(), nil)
	if err != nil {
		return fmt.Errorf("unable to create hijack request: %s", err)
	}

	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")

	conn, err := ros.newConn()
	if err != nil {
		return err
	}

	defer conn.Close()

	// Server also hijacks the connection, error 'connection closed'
	// expected.
	httputil.NewClientConn(conn, nil).Do(req)

	buf := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	fetcher := newRemoteObjectFetcher(buf)

	return ros.r.fetchObjects(fetcher, desc, progress)
}

func (r *Repository) HandleServeObjects(rw http.ResponseWriter, req *http.Request) {
	hijacker, ok := rw.(http.Hijacker)
	if !ok {
		log.Print("http hijacking not supported")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	conn, buf, err := hijacker.Hijack()
	if err != nil {
		log.Printf("unable to hijack connection :%s", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer conn.Close()

	fmt.Fprint(conn, "HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")

	if err := r.serveObjects(buf, &ProgressMeter{}); err != nil {
		log.Printf("unable to serve objects: %s", err)
	}
}

func (ros *remoteObjectStore) Push(desc Descriptor, progress *ProgressMeter) error {
	query := url.Values{}
	query.Set("service", "receive-objects")

	reqURL := new(url.URL)
	*reqURL = *ros.baseURL
	reqURL.RawQuery = query.Encode()

	req, err := http.NewRequest("POST", reqURL.String(), nil)
	if err != nil {
		return fmt.Errorf("unable to create receive-objects request: %s", err)
	}

	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")

	conn, err := ros.newConn()
	if err != nil {
		return err
	}

	defer conn.Close()

	// Server also hijacks the connection, error 'connection closed'
	// expected.
	httputil.NewClientConn(conn, nil).Do(req)

	buf := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// First, send the descriptor for the object we'd like to upload.
	if err := MarshalDescriptor(buf, desc); err != nil {
		return fmt.Errorf("unable to encode descriptor: %s", err)
	}

	if err := buf.Flush(); err != nil {
		return fmt.Errorf("unable to flush descriptor buffer: %s", err)
	}

	return ros.r.serveObjects(buf, progress)
}

func (r *Repository) HandleReceiveObjects(rw http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	tag := req.Form.Get("tag")

	hijacker, ok := rw.(http.Hijacker)
	if !ok {
		log.Print("http hijacking not supported")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	conn, buf, err := hijacker.Hijack()
	if err != nil {
		log.Printf("unable to hijack connection :%s", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer conn.Close()

	fmt.Fprint(conn, "HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")

	// First, read a descriptor for the object that the remote would like
	// to upload.
	desc, err := UnmarshalDescriptor(buf)
	if err != nil {
		log.Printf("unable to read descriptor requested by remote: %s", err)
		return
	}

	// Get a remote object fetcher.
	fetcher := newRemoteObjectFetcher(buf)

	if r.Contains(desc.Digest()) {
		fetcher.SkipObject(desc)
		return
	}

	if err := r.fetchObjects(fetcher, desc, &ProgressMeter{}); err != nil {
		log.Printf("unable to fetch objects: %s", err)
	}

	r.TagStore().Set(tag, desc)
}
