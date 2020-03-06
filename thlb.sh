#!/usr/bin/env bash

PROJECT_ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
HLB_SCRIPT=${PROJECT_ROOT}/build/titus.hlb
mkdir -p ${PROJECT_ROOT}/build && rm -f ${HLB_SCRIPT}

# HLB does not support environment variables yet (see https://github.com/openllb/hlb/issues/2)
sed -e "s/@{{USER}}/${USER}/g" ${PROJECT_ROOT}/scripts/hlb/titus.hlb.template > ${HLB_SCRIPT}

hlb run --target titusBuild ${HLB_SCRIPT}
