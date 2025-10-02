package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster"
)

type Application struct {
	httpAddr string
	grpcAddr string
	node     *cluster.ZenNode
}

type request struct {
	t           testing.TB
	ctx         context.Context
	method      string
	path        string
	addr        string
	requestBody interface{}
	headers     http.Header
	transport   http.RoundTripper
}

func (app *Application) NewRequest(t testing.TB) *request {
	return &request{
		t:           t,
		ctx:         nil,
		method:      "GET",
		path:        "",
		addr:        app.httpAddr,
		requestBody: nil,
		headers:     map[string][]string{},
		transport:   &http.Transport{},
	}
}

func (r *request) WithHeaders(headers http.Header) *request {
	r.headers = headers
	return r
}

func (r *request) WithContext(ctx context.Context) *request {
	r.ctx = ctx
	return r
}

func (r *request) WithHeader(key string, value string) *request {
	r.headers[key] = []string{value}
	return r
}

func (r *request) WithMethod(method string) *request {
	r.method = method
	return r
}

func (r *request) WithPath(path string) *request {
	r.path = path
	return r
}

func (r *request) WithBody(body interface{}) *request {
	r.requestBody = body
	r.headers.Set("Content-Type", "application/json")
	return r
}

func (r *request) Do() ([]byte, int, *http.Response, error) {
	c := http.Client{
		Transport: r.transport,
	}
	var reader io.Reader
	if r.requestBody != nil {
		if r.headers.Get("Content-Type") == "application/json" {
			json, err := json.Marshal(r.requestBody)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("could not serialize %T to json", r.requestBody)
			}
			reader = bytes.NewBuffer(json)
		} else {
			reader = bytes.NewBuffer(r.requestBody.([]byte))
		}
	}
	reqCtx := context.Background()
	if r.t != nil {
		reqCtx = r.t.Context()
	}
	if r.ctx != nil {
		reqCtx = r.ctx
	}
	req, err := http.NewRequestWithContext(reqCtx, r.method, fmt.Sprintf("http://%s%s", r.addr, r.path), reader)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("error during request build: %w", err)
	}
	if r.headers != nil {
		req.Header = r.headers
	}
	res, err := c.Do(req)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("error during request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			panic(fmt.Sprintf("Could not read response body %v", err))
		}
	}(res.Body)
	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("could not read response body: %w", err)
	}
	return bytes, res.StatusCode, res, nil
}

func (r *request) DoOk() ([]byte, error) {
	respBody, _, resp, err := r.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}
	err = AssertCommon(r.t, resp)
	if err != nil {
		return nil, fmt.Errorf("error during response %s validation: %w", string(respBody), err)
	}
	return respBody, nil
}

// AssertCommon will check if status code is 2XX and that the content-type header is set to application/json; charset=utf-8
func AssertCommon(t testing.TB, resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("expected status code 2XX, got %v", resp.StatusCode)
	}

	val, ok := resp.Header["Content-Type"]

	// Assert that the "content-type" header is actually set
	if !ok {
		return fmt.Errorf("expected Content-Type header to be set")
	}

	// Assert that it was set as expected
	if val[0] != "application/json" {
		return fmt.Errorf("expected \"application/json\", got %s", val[0])
	}
	return nil
}
