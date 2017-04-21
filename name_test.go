package goavro

// NOTE: part of goavro package because it tests private functionality

import (
	"testing"
)

func TestNameStartsInvalidCharacter(t *testing.T) {
	_, err := NewName("&X", "org.foo", "")
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameContainsInvalidCharacter(t *testing.T) {
	_, err := NewName("X", "org.foo&bar", "")
	if _, ok := err.(ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameAndNamespaceProvided(t *testing.T) {
	n, err := NewName("X", "org.foo", "")
	if err != nil {
		t.Fatal(err)
	}

	// fullname: org.foo.X

	if actual, expected := n.FullName, "org.foo.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	// // name: X
	// if actual, expected := n.Name, "X"; actual != expected {
	// 	t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	// }
	// namespace: org.foo
	if actual, expected := n.Namespace, "org.foo"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestNameWithDotIgnoresNamespace(t *testing.T) {
	n, err := NewName("org.bar.X", "some.ignored.namespace", "")
	if err != nil {
		t.Fatal(err)
	}

	// fullname: org.foo.X
	if actual, expected := n.FullName, "org.bar.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	// // name: X
	// if actual, expected := n.Name, "X"; actual != expected {
	// 	t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	// }
	// namespace: org.foo
	if actual, expected := n.Namespace, "org.bar"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestNameWithoutDotsButWithEmptyNamespaceAndEnclosingName(t *testing.T) {
	n, err := NewName("X", "", "org.foo")
	if err != nil {
		t.Fatal(err)
	}

	// fullname: org.foo.X
	if actual, expected := n.FullName, "org.foo.X"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	// // name: X
	// if actual, expected := n.Name, "X"; actual != expected {
	// 	t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	// }
	// namespace: org.foo
	if actual, expected := n.Namespace, "org.foo"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestBuildCodecString(t *testing.T) {
	codec, err := NewCodec(`{"type":"null"}`)
	if err != nil {
		t.Fatal(err)
	}
	_ = codec
}
