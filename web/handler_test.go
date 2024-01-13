package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/google/uuid"
	"github.com/jba/muxpatterns"
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

func TestShowSubscriptionHandler(t *testing.T) {
	// Should start with no subscriptions
	db := core.OpenTestDatabase(t)
	defer db.Close()
	hc := HandlerContext{Db: db}

	// Show subscription that doesn't exist, should return a 404
	sub, found := showSubscription(t, hc, uuid.New())
	assert.False(t, found)

	// Create a subscription
	sub = createSubscription(t, hc)

	// Find the subscription
	subFound, found := showSubscription(t, hc, sub.ID)
	assert.True(t, found)
	assert.Equal(t, sub.ID, subFound.ID)

	// Convert both to JSON, and compare
	subJSON, err := json.Marshal(sub)
	if err != nil {
		t.Fatal(err)
	}
	subFoundJSON, err := json.Marshal(subFound)
	if err != nil {
		t.Fatal(err)
	}
	opts := jsondiff.DefaultJSONOptions()
	if diff, diffStr := jsondiff.Compare(subFoundJSON, subJSON, &opts); diff != jsondiff.FullMatch {
		t.Logf("Expected: %s", string(subJSON))
		t.Logf("Actual: %s", string(subFoundJSON))
		t.Logf("Difference: %s", diffStr)
		t.Logf("DifferenceType: %s", diff)
		t.Fail()
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

func showSubscription(t *testing.T, hc HandlerContext, id uuid.UUID) (sub view.Subscription, found bool) {
	found = false

	mux := muxpatterns.NewServeMux()
	mux.HandleFunc("GET /1/subscriptions/{id}", func(w http.ResponseWriter, r *http.Request) {
		hc.ShowSubscriptionHandler(w, r, context.Background())
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + fmt.Sprintf("/1/subscriptions/%s", id.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	status := resp.StatusCode
	switch status {
	case http.StatusOK:
		// OK, found
		found = true
		if err = json.Unmarshal(body, &sub); err != nil {
			t.Fatal(err)
		}
	case http.StatusNotFound:
		// Not found
	default:
		// Unexpected status
		t.Fatalf("handler returned wrong status code: got %v want 200 or 404",
			status)
	}
	return sub, found
}
