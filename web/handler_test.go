package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Change directory to the root of the project, so that the relative paths in the tests work
	err := os.Chdir("..")
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestPostSubscriptionHandlerResponse(t *testing.T) {
	db := core.OpenTestDatabase(t)
	defer db.Close()
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
			if status := rr.Code; status != http.StatusCreated {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, http.StatusCreated)
			}

			// Check that the response is a superset of the expected response
			opts := jsondiff.DefaultJSONOptions()
			if diff, diffStr := jsondiff.Compare(rr.Body.Bytes(), expected, &opts); diff != jsondiff.SupersetMatch {
				t.Logf("Expected: %s", expected)
				t.Logf("Actual: %s", rr.Body.String())
				t.Logf("Difference: %s", diffStr)
				t.Logf("DifferenceType: %s", diff)
				t.Fail()
			}
		})
	}
}

func TestListSubscriptionsHandler(t *testing.T) {
	// Should start with no subscriptions
	db := core.OpenTestDatabase(t)
	defer db.Close()
	hc := HandlerContext{Db: db}

	// Make a request to list subscriptions, should be none
	subs := listSubscriptions(t, hc)
	assert.Len(t, subs.Subscriptions, 0)

	// Create a subscription
	created := make([]view.Subscription, 0)
	sub := createSubscription(t, hc)
	created = append(created, sub)

	// Make a request to list subscriptions, should be one
	subs = listSubscriptions(t, hc)
	assert.Len(t, subs.Subscriptions, 1)
	assert.Equal(t, sub.ID, subs.Subscriptions[0].ID)

	// Create a hundred subscriptions
	for i := 0; i < 100; i++ {
		created = append(created, createSubscription(t, hc))
	}

	// Make a request to list subscriptions, should return all created
	subs = listSubscriptions(t, hc)
	require.Equal(t, len(subs.Subscriptions), len(created))
	for i := 0; i < len(created); i++ {
		assert.Equal(t, created[i].ID, subs.Subscriptions[i].ID)
	}
}

func createSubscription(t *testing.T, hc HandlerContext) view.Subscription {
	input, err := os.ReadFile(filepath.Join("testdata", "new-subscription.json"))
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("POST", "1/subscriptions", bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	hc.PostSubscriptionHandler(rr, req, context.Background())

	if status := rr.Code; status != http.StatusCreated {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}

	sub := view.Subscription{}
	if err = json.Unmarshal(rr.Body.Bytes(), &sub); err != nil {
		t.Fatal(err)
	}
	return sub
}

func listSubscriptions(t *testing.T, hc HandlerContext) view.SubscriptionCollection {
	req, err := http.NewRequest("GET", "1/subscriptions?limit=10000&offset=0", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	hc.ListSubscriptionsHandler(rr, req, context.Background())

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	subs := view.SubscriptionCollection{}
	if err = json.Unmarshal(rr.Body.Bytes(), &subs); err != nil {
		t.Fatal(err)
	}
	return subs
}
