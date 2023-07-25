package gibero

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"time"
)

var exponentMask int = 0x40
var NEGATIVE_FLAG byte = 0xF0
var NUM_OFFSET byte = 0x80

const (
	maxConvertibleInt    = (1 << 63) - 1
	maxConvertibleNegInt = (1 << 63)
)

func addDigitToMantissa(mantissaIn uint64, d byte) (mantissaOut uint64, carryOut bool) {
	var carry uint64
	mantissaOut = mantissaIn

	if mantissaIn != 0 {
		var over uint64
		over, mantissaOut = bits.Mul64(mantissaIn, uint64(10))
		if over != 0 {
			return mantissaIn, true
		}
	}
	mantissaOut, carry = bits.Add64(mantissaOut, uint64(d), carry)
	if carry != 0 {
		return mantissaIn, true
	}
	return mantissaOut, false
}
func ToNumber(mantissa []byte, negative bool, exponent int) []byte {

	if len(mantissa) == 0 {
		return []byte{128}
	}

	if exponent%2 == 0 {
		mantissa = append([]byte{'0'}, mantissa...)
	}

	mantissaLen := len(mantissa)
	size := 1 + (mantissaLen+1)/2
	if negative && mantissaLen < 21 {
		size++
	}
	buf := make([]byte, size, size)

	for i := 0; i < mantissaLen; i += 2 {
		b := 10 * (mantissa[i] - '0')
		if i < mantissaLen-1 {
			b += mantissa[i+1] - '0'
		}
		if negative {
			// b = 100 - b
			buf[1+i/2] = NUM_OFFSET - b
		} else {
			buf[1+i/2] = b + NUM_OFFSET

		}
	}

	if negative && mantissaLen < 21 {
		buf[len(buf)-1] = NEGATIVE_FLAG
	}

	if exponent < 0 {
		exponent--
	}
	exponent = (exponent / 2) + 1
	if negative {
		buf[0] = byte(exponent+exponentMask) ^ 0x7f
	} else {
		buf[0] = byte(exponent+exponentMask) | 0x80
	}
	return buf
}

func FromNumber(inputData []byte) (mantissa uint64, negative bool, exponent int, mantissaDigits int, err error) {
	if len(inputData) == 0 {
		return 0, false, 0, 0, fmt.Errorf("Invalid NUMBER")
	}
	if inputData[0] == 0x80 {
		return 0, false, 0, 0, nil
	}
	negative = inputData[0]&0x80 == 0
	if negative {
		exponent = int(inputData[0]^0x7f) - exponentMask
	} else {
		exponent = int(inputData[0]&0x7f) - exponentMask
	}

	buf := inputData[1:]
	// When negative, strip the last byte if equal 0x66
	if negative && inputData[len(inputData)-1] == NEGATIVE_FLAG {
		buf = inputData[1 : len(inputData)-1]
	}

	carry := false // get true when mantissa exceeds 64 bits
	firstDigitWasZero := 0

	// Loop on mantissa digits, stop with the capacity of int64 is reached
	// Beyond, digits will be lost during convertion t
	mantissaDigits = 0
	for p, digit100 := range buf {
		if p == 0 {
			firstDigitWasZero = -1
		}
		if negative {
			digit100 = NUM_OFFSET - digit100
		} else {
			digit100 = digit100 - NUM_OFFSET
		}

		mantissa, carry = addDigitToMantissa(mantissa, digit100/10)
		if carry {
			break
		}
		mantissaDigits++

		mantissa, carry = addDigitToMantissa(mantissa, digit100%10)
		if carry {
			break
		}
		mantissaDigits++
	}
	// exponent = -4
	exponent = exponent*2 - mantissaDigits // Adjust exponent to the retrieved mantissa
	printFormat("decode exponent [%d]\n", exponent)
	return mantissa, negative, exponent, mantissaDigits + firstDigitWasZero, nil
}

func EncodeInt64(val int64) []byte {
	mantissa := []byte(strconv.FormatInt(val, 10))
	negative := mantissa[0] == '-'
	if negative {
		mantissa = mantissa[1:]
	}
	exponent := len(mantissa) - 1
	trailingZeros := 0
	for i := len(mantissa) - 1; i >= 0 && mantissa[i] == '0'; i-- {
		trailingZeros++
	}
	mantissa = mantissa[:len(mantissa)-trailingZeros]
	return ToNumber(mantissa, negative, exponent)
}

func EncodeUint64(val uint64) []byte {
	mantissa := []byte(strconv.FormatUint(val, 10))
	exponent := len(mantissa) - 1
	trailingZeros := 0
	for i := len(mantissa) - 1; i >= 0 && mantissa[i] == '0'; i-- {
		trailingZeros++
	}
	mantissa = mantissa[:len(mantissa)-trailingZeros]
	return ToNumber(mantissa, false, exponent)
}

func EncodeInt(val int) []byte {
	return EncodeInt64(int64(val))
}
func EncodeFloat(num float64, bitSize int) ([]byte, error) {
	if num == 0.0 {
		return []byte{128}, nil
	}

	var (
		exponent int
		err      error
	)
	mantissa := []byte(strconv.FormatFloat(num, 'e', -1, bitSize))
	if i := bytes.Index(mantissa, []byte{'e'}); i >= 0 {
		exponent, err = strconv.Atoi(string(mantissa[i+1:]))
		if err != nil {
			return nil, err
		}
		mantissa = mantissa[:i]
	}
	negative := mantissa[0] == '-'
	if negative {
		mantissa = mantissa[1:]
	}
	if i := bytes.Index(mantissa, []byte{'.'}); i >= 0 {
		mantissa = append(mantissa[:i], mantissa[i+1:]...)
	}
	return ToNumber(mantissa, negative, exponent), nil
}

func DecodeInt(inputData []byte) int64 {
	mantissa, negative, exponent, _, err := FromNumber(inputData)
	if err != nil || exponent < 0 {
		return 0
	}

	for exponent > 0 {
		mantissa *= 10
		exponent--
	}
	if negative && (mantissa>>63) == 0 {
		return -int64(mantissa)
	}
	return int64(mantissa)
}

func DecodeDouble(inputData []byte) float64 {
	mantissa, negative, exponent, _, err := FromNumber(inputData)
	if err != nil {
		return math.NaN()
	}
	absExponent := int(math.Abs(float64(exponent)))
	if negative {
		return -math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
	}
	return math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)

}
func DecodeNumber(inputData []byte) interface{} {
	var powerOfTen = [...]uint64{
		1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
		10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000,
		1000000000000000, 10000000000000000, 100000000000000000, 1000000000000000000,
		10000000000000000000}

	mantissa, negative, exponent, mantissaDigits, err := FromNumber(inputData)
	if err != nil {
		return math.NaN()
	}

	if mantissaDigits == 0 {
		return int64(0)
	}

	if exponent >= 0 && exponent < len(powerOfTen) {
		// exponent = mantissaDigits - exponent
		IntMantissa := mantissa
		IntExponent := exponent
		var over uint64
		over, IntMantissa = bits.Mul64(IntMantissa, powerOfTen[IntExponent])
		if (!negative && IntMantissa > maxConvertibleInt) ||
			(negative && IntMantissa > maxConvertibleNegInt) {
			goto fallbackToFloat
		}
		if over != 0 {
			goto fallbackToFloat
		}

		if negative && (IntMantissa>>63) == 0 {
			return -int64(IntMantissa)
		}
		return int64(IntMantissa)
	}

fallbackToFloat:
	//if negative {
	//	return -float64(mantissa) * math.Pow10(exponent)
	//}
	//
	//return float64(mantissa) * math.Pow10(exponent)
	absExponent := int(math.Abs(float64(exponent)))
	if negative {
		return -math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
	}
	return math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
}

func fromTimestamps(ret []byte, ts int64) {
	ti := time.Unix(ts/1000, (ts%1000)*1000000)
	fromTimestamp(ret, &ti)
}

func fromTimestamp(ret []byte, ti *time.Time) {
	ret[0] = uint8(ti.Year()/100 + 100)
	ret[1] = uint8(ti.Year()%100 + 100)
	ret[2] = uint8(ti.Month())
	ret[3] = uint8(ti.Day())
	ret[4] = uint8(ti.Hour())
	ret[5] = uint8(ti.Minute())
	ret[6] = uint8(ti.Second())
	ret[7] = 0
	binary.BigEndian.PutUint32(ret[8:12], uint32(ti.Nanosecond()))
}

func toTimestamp(ts []byte) *time.Time {
	var year int
	year = (int(255&ts[0])-100)*100 + (int(255&ts[1]) - 100)
	m := time.Month(int(255 & ts[2]))
	date := time.Date(year, m, int(255&ts[3]), int(255&ts[4]), int(255&ts[5]), int(255&ts[6]), int(binary.BigEndian.Uint32(ts[8:12])), time.Local)
	return &date
}

func fromDates(ret []byte, ts int64) {
	ti := time.Unix(ts/1000, (ts%1000)*1000000)
	fromDate(ret, &ti)
}
func fromDate(ret []byte, ti *time.Time) {
	ret[0] = uint8(ti.Year()/100 + 100)
	ret[1] = uint8(ti.Year()%100 + 100)
	ret[2] = uint8(ti.Month())
	ret[3] = uint8(ti.Day())
	ret[4] = 0
	ret[5] = 0
	ret[6] = 0
	ret[7] = 0
	// ret[4] = uint8(ti.Hour())
	// ret[5] = uint8(ti.Minute())
	// ret[6] = uint8(ti.Second())
}

func toDate(ts []byte) *time.Time {
	var year int
	year = (int(255&ts[0])-100)*100 + (int(255&ts[1]) - 100)
	m := time.Month(int(255 & ts[2]))
	date := time.Date(year, m, int(255&ts[3]), int(255&ts[4]), int(255&ts[5]), int(255&ts[6]), 0, time.Local)
	return &date
}
