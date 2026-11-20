package main

import "testing"

func TestWebhookEventsDefaults(t *testing.T) {
	if got := eventNames(webhookEvents("", "")); got != "inbox.batch,session.logged_out,session.restored" {
		t.Fatalf("default: %s", got)
	}
	if got := eventNames(webhookEvents("", "0")); got != "inbox.new,session.logged_out,session.restored" {
		t.Fatalf("no batching: %s", got)
	}
	if got := eventNames(webhookEvents("*", "")); got != "*" {
		t.Fatalf("all: %s", got)
	}
	if got := eventNames(webhookEvents("message, message.ack", "")); got != "message,message.ack" {
		t.Fatalf("list: %s", got)
	}
}
