package goavro

// NOTE: part of goavro package because it tests private functionality

import (
	"testing"
)

func TestNameStartsInvalidCharacter(t *testing.T) {
	_, err := newName("&X", "org.foo", nullNamespace)
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameContainsInvalidCharacter(t *testing.T) {
	_, err := newName("X", "org.foo&bar", nullNamespace)
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameAndNamespaceProvided(t *testing.T) {
	n, err := newName("X", "org.foo", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.foo.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.foo"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestNameWithDotIgnoresNamespace(t *testing.T) {
	n, err := newName("org.bar.X", "some.ignored.namespace", nullNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.bar.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.bar"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestNameWithoutDotsButWithEmptyNamespaceAndEnclosingName(t *testing.T) {
	n, err := newName("X", nullNamespace, "org.foo")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := n.fullName, "org.foo.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := n.namespace, "org.foo"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}
