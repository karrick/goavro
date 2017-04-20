package goavro_test

import (
	"testing"

	"github.com/karrick/goavro"
)

func TestNameStartsInvalidCharacter(t *testing.T) {
	n, err := goavro.NewName("&X", "org.foo", "")
	if n.FullName != "" && n.Namespace != "" {
		t.Errorf("Actual: %#v, Expected: %#v", n, nil)
	}
	if _, ok := err.(goavro.ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, goavro.ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameContainsInvalidCharacter(t *testing.T) {
	n, err := goavro.NewName("X", "org.foo&bar", "")
	if n.FullName != "" && n.Namespace != "" {
		t.Errorf("Actual: %#v, Expected: %#v", n, nil)
	}
	if _, ok := err.(goavro.ErrInvalidName); err == nil && !ok {
		t.Errorf("Actual: %#v, Expected: %#v", err, goavro.ErrInvalidName{"start with [A-Za-z_]"})
	}
}

func TestNameAndNamespaceProvided(t *testing.T) {
	n, err := goavro.NewName("X", "org.foo", "")
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
	n, err := goavro.NewName("org.bar.X", "some.ignored.namespace", "")
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
	n, err := goavro.NewName("X", "", "org.foo")
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
	codec, err := goavro.NewCodec(`{"type":"null"}`)
	if err != nil {
		t.Fatal(err)
	}

	_ = codec
}
