#!/usr/bin/env bash

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    VERSION="1.8.0"
    URL="https://github.com/Orange-OpenSource/hurl/releases/download/$VERSION/hurl-$VERSION-x86_64-linux.tar.gz"
    INSTALL_DIR="/var/tmp"
    curl -sL $URL | tar xvz -C $INSTALL_DIR
    sudo mv -fv $INSTALL_DIR/hurl-1.8.0/hurl /usr/bin && chmod +x /usr/bin/hurl
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install hurl
fi
