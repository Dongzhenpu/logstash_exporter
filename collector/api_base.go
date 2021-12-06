package collector

import (
	"encoding/json"
	"github.com/go-kit/log/level"
	"net/http"
	"os"

	"github.com/go-kit/log"
)

// HTTPHandler type
type HTTPHandler struct {
	Endpoint string
}

// Get method for HTTPHandler
func (h *HTTPHandler) Get() (http.Response, error) {
	response, err := http.Get(h.Endpoint)
	if err != nil {
		return http.Response{}, err
	}

	return *response, nil
}

// HTTPHandlerInterface interface
type HTTPHandlerInterface interface {
	Get() (http.Response, error)
}

func getMetrics(h HTTPHandlerInterface, target interface{}) error {
	w := log.NewSyncWriter(os.Stdout)
	logger := log.NewLogfmtLogger(w)
	response, err := h.Get()
	if err != nil {
		level.Error(logger).Log("msg", "Cannot retrieve metrics", "err", err)
		return err
	}

	defer func() {
		if err := response.Body.Close(); err != nil {
			level.Error(logger).Log("msg", "Cannot close response body", "err", err)
		}
	}()

	err = json.NewDecoder(response.Body).Decode(target)
	if err != nil {
		level.Error(logger).Log("msg", "Cannot parse Logstash response json", "err", err)
	}

	return err
}
