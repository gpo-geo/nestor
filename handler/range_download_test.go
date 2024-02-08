package handler

import (
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestParseRangeOk(t *testing.T) {
    assert := assert.New(t)

    var start int64
    var length int64
    var err error

    start, length, err = parseRange("")
    assert.Nil(err)
    assert.Equal(start, int64(0))
    assert.Equal(length, int64(-1))

    start, length, err = parseRange("bytes=0-9")
    assert.Nil(err)
    assert.Equal(start, int64(0))
    assert.Equal(length, int64(10))
    
    start, length, err = parseRange("bytes=-10")
    assert.Nil(err)
    assert.Equal(start, int64(-10))
    assert.Equal(length, int64(-1))

}

func TestParseRangeFails(t *testing.T) {
    assert := assert.New(t)

    var err error

    _, _, err = parseRange("something")
    assert.NotNil(err)
    assert.Equal("invalid range", err.Error())
    
    _, _, err = parseRange("bytes=10-abc")
    assert.NotNil(err)
    assert.Equal("invalid range", err.Error())
    
    _, _, err = parseRange("bytes=0-9,20-32")
    assert.NotNil(err)
    assert.Equal("multiple ranges not supported", err.Error())
    
    _, _, err = parseRange("bytes=10-6")
    assert.NotNil(err)
    assert.Equal("invalid range", err.Error())
    
    _, _, err = parseRange("bytes=--5")
    assert.NotNil(err)
    assert.Equal("invalid range", err.Error())
}


