#!/bin/sh

version=`git describe --tags`
cat << EOF > version.go
package migrator 

//go:generate sh ./get_version.sh
var Version = "$version"
EOF

# Inspired/copied from: https://adrianhesketh.com/2016/09/04/adding-a-version-number-to-go-packages-with-go-generate/
