package goavro

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidName is the error returned when one or more parts of an Avro name is invalid.
type ErrInvalidName struct {
	Message string
}

func (e ErrInvalidName) Error() string {
	return "The name portion of a fullname, record field names, and enum symbols must " + e.Message
}

// NOTE: This function designed to work with name components, after they have been split on the
// period rune.
func isRuneInvalidForFirstCharacter(r rune) bool {
	// if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || r == '.' {

	if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' {
		return false
	}
	return true
}

func isRuneInvalidForOtherCharacters(r rune) bool {
	if r >= '0' && r <= '9' {
		return false
	}
	return isRuneInvalidForFirstCharacter(r)
}

func checkNameComponent(s string) error {
	if len(s) == 0 {
		return &ErrInvalidName{"not be empty"}
	}
	if strings.IndexFunc(s[:1], isRuneInvalidForFirstCharacter) != -1 {
		return &ErrInvalidName{("start with [A-Za-z_]: " + s)}
	}
	if strings.IndexFunc(s[1:], isRuneInvalidForOtherCharacters) != -1 {
		return &ErrInvalidName{("have second and remaining characters contain only [A-Za-z0-9_]: " + s)}
	}
	return nil
}

// Name describes an Avro name in terms of its full name and namespace.
type Name struct {
	FullName  string // the instance's Avro name
	Namespace string // for use when building new name from existing one
}

// NewName returns a new Name instance after first ensuring the arguments do not violate any of the
// Avro naming rules.
func NewName(name, namespace, enclosingNamespace string) (*Name, error) {
	var n Name

	if index := strings.LastIndexByte(name, '.'); index > -1 {
		// inputName does contain a dot, so ignore everything else and use it as the full name
		n.FullName = name
		n.Namespace = name[:index]
	} else {
		// inputName does not contain a dot, therefore is not the full name
		if namespace != "" {
			// if namespace provided in the schema in the same schema level, use it
			n.FullName = namespace + "." + name
			n.Namespace = namespace
		} else if enclosingNamespace != "" {
			// otherwise if enclosing namespace provided, use it
			n.FullName = enclosingNamespace + "." + name
			n.Namespace = enclosingNamespace
		} else {
			// otherwise no namespace, so use null namespace, the empty string
			n.FullName = name
		}
	}

	// verify all components of the full name for adherence to Avro naming rules
	for _, component := range strings.Split(n.FullName, ".") {
		if err := checkNameComponent(component); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

func newNameFromSchemaMap(enclosingNamespace string, schemaMap map[string]interface{}) (*Name, error) {
	var nameString, namespaceString string

	name, ok := schemaMap["name"]
	if !ok {
		return nil, errors.New("cannot create name: ought to have name key")
	}
	nameString, ok = name.(string)
	if !ok || nameString == "" {
		return nil, fmt.Errorf("cannot create name: name value ought to be non-empty string; received: %T", name)
	}
	namespace, ok := schemaMap["namespace"]
	if ok {
		namespaceString, ok = namespace.(string)
		if !ok || namespaceString == "" {
			return nil, fmt.Errorf("cannot crate name: namespace value ought to be non-empty string; received: %T", namespace)
		}
	}

	return NewName(nameString, namespaceString, enclosingNamespace)
}

// Equal returns true when two Name instances refer to the same Avro name; otherwise it returns
// false.
func (n Name) Equal(n2 Name) bool {
	return n.FullName == n2.FullName
}

func (n Name) String() string {
	return n.FullName
}

// short returns the name without the prefixed namespace.
func (n Name) short() string {
	if index := strings.LastIndexByte(n.FullName, '.'); index > -1 {
		return n.FullName[index+1:]
	}
	return n.FullName
}
