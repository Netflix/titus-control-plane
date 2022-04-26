#!/usr/bin/env bash

function go2java() {
    # Strip off extension and add java
    output="${1%.*}.java"
    # Capitalize first letter
    output="${output^}"
    getHeader > $output
    getBody $1 >> $output
    getFooter >> $output
}

function getBody() {
    filename=$1
    constants="$(awk '/^const/{flag=1; next} /^)/{flag=0} flag' $filename)"
    echo "$constants" | sed 's/^\tAnnotation.*/\tpublic static final String &;/g'
}

function getHeader() {
cat << EOF
// This file is autogenerated!!!
// DO NOT EDIT
//
// Instead make changes to titus-kube-common
// https://github.com/Netflix/titus-kube-common/blob/master/pod/annotations.go
// And then run make refresh in this directory
// 
package com.netflix.titus.common.kube;

public class Annotations {

EOF
}

function getFooter() {
cat << EOF
}
EOF
}


go2java "annotations.podcommon"