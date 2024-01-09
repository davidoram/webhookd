package web

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/davidoram/webhookd/core"
)

func TestPostSubscriptionHandler(t *testing.T) {
	db := core.OpenTestDatabase(t)
	hc := HandlerContext{Db: db}
	cases := []string{"new-subscription.json"}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			input, err := os.ReadFile(filepath.Join("testdata", c))
			if err != nil {
				t.Fatal(err)
			}
			expected, err := os.ReadFile(filepath.Join("testdata", c+".expected"))
			if err != nil {
				t.Fatal(err)
			}

			// Create a request
			req, err := http.NewRequest("POST", "1/subscriptions", bytes.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}

			// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
			rr := httptest.NewRecorder()

			// Call the handler function
			hc.PostSubscriptionHandler(rr, req, context.Background())

			// Check the status code is what we expect.
			if status := rr.Code; status != http.StatusOK {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, http.StatusOK)
			}

			if rr.Body.String() != string(expected) {
				t.Errorf("handler returned unexpected body: got %v want %v",
					rr.Body.String(), string(expected))
			}
		})
	}
}
