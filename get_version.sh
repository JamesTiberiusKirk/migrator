#!/bin/sh

# Get the version.
version=`git describe --tags --long`
# Write out the package.
cat << EOF > version.go
package migrator 

//go:generate sh ./get_version.sh
var Version = "$version"
EOF
