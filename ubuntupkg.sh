#!/bin/bash
VERSION=$(cat VERSION)
cp -r debian Monaco_${VERSION}
cp -r src/* Monaco_${VERSION}/usr/monaco/
dpkg-deb --build Monaco_${VERSION}
rm -rf Monaco_${VERSION}
