#include <stdint.h>

#if defined(__linux__) || defined(__CYGWIN__)
#include <endian.h>
#elif defined(__NetBSD__) || defined(__FreeBSD__) || defined(__OpenBSD__) \
      || defined(__DragonFly__)
#include <sys/endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#elif defined(_WIN32) || defined(_WIN64) || defined(__WINDOWS__)
/* Assume Windows is always LE.  There seems to be no reliable way
   to detect endianness there */
#define __LITTLE_ENDIAN 1234
#define __BIG_ENDIAN 4321
#define __BYTE_ORDER __LITTLE_ENDIAN
#endif

#if defined(_BYTE_ORDER) && !defined(__BYTE_ORDER)
#define __BYTE_ORDER _BYTE_ORDER
#endif

#if defined(BYTE_ORDER) && !defined(__BYTE_ORDER)
#define __BYTE_ORDER BYTE_ORDER
#endif

#if defined(_LITTLE_ENDIAN) && !defined(__LITTLE_ENDIAN)
#define __LITTLE_ENDIAN _LITTLE_ENDIAN
#endif

#if defined(LITTLE_ENDIAN) && !defined(__LITTLE_ENDIAN)
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#endif

#if defined(_BIG_ENDIAN) && !defined(__BIG_ENDIAN)
#define __BIG_ENDIAN _BIG_ENDIAN
#endif

#if defined(BIG_ENDIAN) && !defined(__BIG_ENDIAN)
#define __BIG_ENDIAN BIG_ENDIAN
#endif

#if !defined(__BYTE_ORDER) || !defined(__LITTLE_ENDIAN) \
    || !defined(__BIG_ENDIAN)
#error Cannot determine platform byte order.
#endif

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)

#define apg_bswap16(x) __builtin_bswap16(x)
#define apg_bswap32(x) __builtin_bswap32(x)
#define apg_bswap64(x) __builtin_bswap64(x)

#elif defined(_MSC_VER)

#define apg_bswap16(x) _byteswap_ushort(x)
#define apg_bswap32(x) _byteswap_ulong(x)
#define apg_bswap64(x) _byteswap_uint64(x)

#else

static inline uint16_t
apg_bswap16(uint16_t)
{
    return ((x << 8) & 0xff00) | (x >> 8) & 0x00ff));
}

static inline uint32_t
apg_bswap32(uint32_t x)
{
    return (
        ((x << 24) & 0xff000000) | ((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) | ((x >> 24) & 0x000000ff)
    );
}

static inline uint64_t
apg_bswap64(uint64_t x)
{
	return (
		((x << 56) & 0xff00000000000000ULL) |
		((x << 40) & 0x00ff000000000000ULL) |
		((x << 24) & 0x0000ff0000000000ULL) |
		((x << 8) & 0x000000ff00000000ULL) |
		((x >> 8) & 0x00000000ff000000ULL) |
		((x >> 24) & 0x0000000000ff0000ULL) |
		((x >> 40) & 0x000000000000ff00ULL) |
		((x >> 56) & 0x00000000000000ffULL);
    );
}

#endif

#if __BYTE_ORDER == __BIG_ENDIAN

#define apg_hton16(x) (x)
#define apg_hton32(x) (x)
#define apg_hton64(x) (x)

#define apg_ntoh16(x) (x)
#define apg_ntoh32(x) (x)
#define apg_ntoh64(x) (x)

#elif __BYTE_ORDER == __LITTLE_ENDIAN

#define apg_hton16(x) apg_bswap16(x)
#define apg_hton32(x) apg_bswap32(x)
#define apg_hton64(x) apg_bswap64(x)

#define apg_ntoh16(x) apg_bswap16(x)
#define apg_ntoh32(x) apg_bswap32(x)
#define apg_ntoh64(x) apg_bswap64(x)

#else

#error Unsupported byte order.

#endif


static inline void
pack_int16(char *buf, int16_t x)
{
    uint16_t nx = apg_hton16((uint16_t)x);
    /* NOTE: the memcpy below is _important_ to support systems
       which disallow unaligned access.  On systems, which do
       allow unaligned access it will be optimized away by the
       compiler
    */
    memcpy(buf, &nx, sizeof(uint16_t));
}


static inline void
pack_int32(char *buf, int64_t x)
{
    uint32_t nx = apg_hton32((uint32_t)x);
    memcpy(buf, &nx, sizeof(uint32_t));
}


static inline void
pack_int64(char *buf, int64_t x)
{
    uint64_t nx = apg_hton64((uint64_t)x);
    memcpy(buf, &nx, sizeof(uint64_t));
}


static inline int16_t
unpack_int16(const char *buf)
{
    uint16_t nx;
    memcpy((char *)&nx, buf, sizeof(uint16_t));
    return (int16_t)apg_ntoh16(nx);
}


static inline int32_t
unpack_int32(const char *buf)
{
    uint32_t nx;
    memcpy((char *)&nx, buf, sizeof(uint32_t));
    return (int32_t)apg_ntoh32(nx);
}


static inline int64_t
unpack_int64(const char *buf)
{
    uint64_t nx;
    memcpy((char *)&nx, buf, sizeof(uint64_t));
    return (int64_t)apg_ntoh64(nx);
}


union _apg_floatconv {
    uint32_t i;
    float f;
};


union _apg_doubleconv {
    uint64_t i;
    double f;
};


static inline void
pack_float(char *buf, float f)
{
    union _apg_floatconv v;
    v.f = f;
    pack_int32(buf, (int32_t)v.i);
}


static inline void
pack_double(char *buf, double f)
{
    union _apg_doubleconv v;
    v.f = f;
    pack_int64(buf, (int64_t)v.i);
}


static inline float
unpack_float(const char *buf)
{
    union _apg_floatconv v;
    v.i = (uint32_t)unpack_int32(buf);
    return v.f;
}


static inline double
unpack_double(const char *buf)
{
    union _apg_doubleconv v;
    v.i = (uint64_t)unpack_int64(buf);
    return v.f;
}
