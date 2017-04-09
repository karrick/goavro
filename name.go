package goavro

import (
	"strings"
)

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

type Name struct {
	Name, Namespace, FullName string
}

func NewName(name, namespace string, enclosing *Name) (*Name, error) {
	n := &Name{
		Name:      name,
		Namespace: namespace,
		FullName:  namespace + "." + name,
	}

	// when name contains dot, ignore namespace parameter (and enclosing namespace?)
	switch index := strings.LastIndexByte(name, '.'); index {
	case -1:
		if namespace != "" {
			n.FullName = namespace + "." + name
			n.Namespace = namespace
			n.Name = name
		} else if enclosing != nil {
			n.FullName = enclosing.Namespace + "." + name
			n.Namespace = enclosing.Namespace
			n.Name = name
		} else {
			n.FullName = name
			n.Namespace = namespace
			n.Name = name
		}
	default:
		// name contains dot, so ignore everything else and use it as the full name
		n.FullName = name
		n.Namespace = name[:index]
		n.Name = name[index+1:]
	}

	if err := checkNameComponent(n.Name); err != nil {
		return nil, err
	}
	if namespace != "" {
		for _, component := range strings.Split(n.Namespace, ".") {
			if err := checkNameComponent(component); err != nil {
				return nil, err
			}
		}
	}

	return n, nil
}

func (n1 Name) Equal(n2 Name) bool {
	return n1.FullName == n2.FullName
}

func (n Name) String() string {
	return n.FullName
}
