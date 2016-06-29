# example: allOfNObjsAre "Immutable" "test/ImmutableClass" "20" "immutable"
function allOfNObjsAre() {
    local TEST=$1
    local KLASS=$2
    local N=$3
    local QUERY=$4
    cat $TEST/$QUERY.out | grep "^$KLASS	1	" > /dev/null || echo "ERROR: all $KLASS objs must be $QUERY"
    cat $TEST/$QUERY.out | grep "^$KLASS	[0-9.]*	$N	0" > /dev/null || echo "ERROR: all $N $KLASS objects must pass $QUERY"
}

# example: noneOfNObjsAre "Immutable" "test/MutableClass" "20" "immutable"
function noneOfNObjsAre() {
    local TEST=$1
    local KLASS=$2
    local N=$3
    local QUERY=$4
    cat $TEST/$QUERY.out | grep "^$KLASS	0	" > /dev/null || echo "ERROR: no $KLASS objs must be $QUERY"
    cat $TEST/$QUERY.out | grep "^$KLASS	[0-9.]*	0	$N" > /dev/null || echo "ERROR: all $N $KLASS objects must fail $QUERY"
}
