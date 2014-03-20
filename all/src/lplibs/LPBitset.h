#ifndef __LP__BITSET__
#define __LP__BITSET__

#include <string.h>

class LPBitset{

public:
	LPBitset(long s);
	~LPBitset();

	void set(long indexBit);
	bool isSet(long indexBit) const;
	void clear(long indexBit);
	void clearAll();

	long size() const;

private:
	long mSize;
	long mNumBytes;
	char *mBytes;
};

#endif
