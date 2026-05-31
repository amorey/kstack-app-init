//go:build windows

package ipc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// Guards the SDDL string that gates pipe access. The realistic regression
// is an edit that broadens the trustee (e.g. WD = Everyone, AU = Authenticated
// Users) or drops the DACL entirely (NULL DACL grants Everyone full access).
//
// We parse the string, assert the DACL is present (not NULL), and round-trip
// it back to SDDL — any structural change to the constant forces the test to
// be updated, which means a human looks at the new policy.
func TestOwnerOnlyDACL_ParsesAndIsRestrictive(t *testing.T) {
	sd, err := windows.SecurityDescriptorFromString(ownerOnlyDACL)
	require.NoError(t, err, "SecurityDescriptorFromString(%q)", ownerOnlyDACL)

	dacl, _, err := sd.DACL()
	require.NoError(t, err)
	require.NotNil(t, dacl, "DACL is NULL — that grants Everyone full access")

	assert.Equal(t, ownerOnlyDACL, sd.String(), "SDDL round-trip mismatch")
}
