#!/bin/bash -e

#############################################################################
# zipkin build script
#
# This script updates the SNAPSHOT version for zipkin to a 
# semver-compliant milestone
#############################################################################

WORKSPACE=$(cd $(dirname $0) && pwd -P)

# List of subdirs we'd like to build
SUBDIRS="zipkin-collector-service zipkin-query-service zipkin-web"
FIRST_SUBDIR=$(echo $SUBDIRS | awk '{print $1}')
REMAINING_SUBDIRS=$(echo $SUBDIRS | sed 's/$FIRST_SUBDIR //')

# Turn off irritating color logging 
export SBT_OPTS="-Dsbt.log.noformat=true"

function version () {
    if [[ -z "$1" ]] ; then 
        exit 1
    fi
    SUBDIR=$1
    VERSION=$($WORKSPACE/bin/sbt zipkin-collector-service/release:version|tail -1|\
      awk '{print $2}')
    echo "$VERSION"
}

# Check if this was triggered by jenkins. If so, it's an official build
if [[ -z $BUILD_NUMBER ]] ; then
    BUILD_NUMBER=dev
    OFFICIAL_BUILD=false
else
    OFFICIAL_BUILD=true
fi

OLD_VERSION=$(version $FIRST_SUBDIR)
if ! $(echo $OLD_VERSION|grep -- '-SNAPSHOT' > /dev/null) ; then
    echo "ERROR: checked in version is not a SNAPSHOT" >&2 
    exit 1
fi

for SUBDIR in $REMAINING_SUBDIRS ; do 
    VERSION=$(version $SUBDIR)
    if [[ $VERSION != $OLD_VERSION ]] ; then
	echo "ERROR: Version mismatch between subdirs" >&2
	exit 1
    fi
    OLD_VERSION=$VERSION
done

SHORT_SHA=$(git rev-parse --short HEAD)

NEW_VERSION=$(echo $OLD_VERSION|sed "s/-SNAPSHOT/\+$BUILD_NUMBER\.$SHORT_SHA/")
VERSION_FILE=$WORKSPACE/project/Project.scala
cp $VERSION_FILE ${VERSION_FILE}.sav
sed  -i "s/.*val.*zipkinVersion.*$OLD_VERSION\"/  val zipkinVersion = \"$NEW_VERSION\"/" $VERSION_FILE

unset SBT_OPTS
for SUBDIR in $SUBDIRS ; do 
    $WORKSPACE/bin/sbt $SUBDIR/package-dist
done

# Restore the edited project file
mv ${VERSION_FILE}.sav $VERSION_FILE
