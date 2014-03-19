#include "LPBitset.h"

static inline long getRequiredBytes(long n){
	long i=n>>3; // n/8
	return (i<<8 == n)?i:i+1;
}

static inline long getByteForBit(long bit){
	return bit>>3; // bit/8
}

static inline long getPosForBit(long bit){
	return 1 << (bit & 0x7); // 1 << (bit%8)
}

LPBitset::LPBitset(long s){
	this->mSize = s;
	this->mNumBytes = getRequiredBytes(s);
	this->mBytes = new char[this->mNumBytes];
	memset(this->mBytes, 0, this->mNumBytes);
}

LPBitset::~LPBitset(){
	delete[] this->mBytes;
}

void LPBitset::set(long indexBit){
	this->mBytes[getByteForBit(indexBit)] = this->mBytes[getByteForBit(indexBit)] | getPosForBit(indexBit);
}

bool LPBitset::isSet(long indexBit) const{
	return (this->mBytes[getByteForBit(indexBit)] & getPosForBit(indexBit) ) > 0;
}

void LPBitset::clear(long indexBit){
	char mask = ~getPosForBit(indexBit);
	this->mBytes[getByteForBit(indexBit)] &= mask;
}

void LPBitset::clearAll(){
	memset(this->mBytes, 0, this->mNumBytes);
}

long LPBitset::size() const{
	return this->mSize;
}



