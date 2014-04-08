/*
 * EfficientTopK.h
 *
 *  Created on: Apr 4, 2014
 *      Author: lambros
 */

#ifndef EFFICIENTTOPK_H_
#define EFFICIENTTOPK_H_


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#ifdef __GNUC__
	#define clz ( x ) __builtin_clz ( x )
	#define ctz ( x ) __builtin_ctz ( x )
#else
	static uint32_t ALWAYS_INLINE popcnt ( uint32_t x ){
		x -=   (( x >>   1 )   &   0x55555555 );
		x =   ((( x >>   2 )   &   0x33333333 )   +   ( x &   0x33333333 ));
		x =   ((( x >>   4 )   + x )   &   0x0f0f0f0f );
		x +=   ( x >>   8 ); x +=   ( x >>   16 );
		return x & 0x0000003f ;
	}

	static uint32_t ALWAYS_INLINE clz ( uint32_t x ){
		x |=   ( x >>   1 );
		x |=   ( x >>   2 );
		x |=   ( x >>   4 );
		x |=   ( x >>   8 );
		x |=   ( x >>   16 );
		return   32   - popcnt ( x );
	}

	static uint32_t ALWAYS_INLINE ctz ( uint32_t x ){
		return popcnt (( x &   - x )   -   1 );
	}
#endif

	static int compare (const void * a, const void * b){
	  return ( *(int*)a - *(int*)b );
	}

	#define REP(i,n) for(int i=0;i<(n);i++ )
	#define W 20
	#define HSH 500

	inline unsigned int hashFM(unsigned int a) {
		a = (a + 0x7ed55d16) + (a << 12);
		a = (a ^ 0xc761c23c) ^ (a >> 19);
		a = (a + 0x165667b1) + (a << 5);
		a = (a + 0xd3a2646c) ^ (a << 9);
		a = (a + 0xfd7046c5) + (a << 3);
		a = (a ^ 0xb55a4f09) ^ (a >> 16);
		return a;
	}

	struct FM {
		int mx[HSH];
		void init() {
			memset(mx, 0, sizeof mx);
		}
		void insert(unsigned int _x) {
			REP(i, HSH) {
				int x = hashFM(_x * i + _x);
				x %= 1 << W;
				mx[i] |= 1 << __builtin_ctz(x);
			}
		};
		void union_FM(FM _t){
			REP(i, HSH){
				mx[i] |= _t.mx[i];
			}
		}
		double count1() {
			//avg of all appox values
			int tot = 0;
			REP(i, HSH) {
				tot += 1 << __builtin_ctz(~mx[i]);
			}
			return tot * 1.0 / HSH;
		}
		double count2() {
			//median of all appox values
			int final[HSH];
			REP(i, HSH) {
				final[i] = 1 << __builtin_ctz(~mx[i]);
			}
			//sort(final, final + HSH);
			qsort (final, HSH, sizeof(int), compare);
			return final[HSH / 2];
		}
		double count3() {
			//avg of all bitmaps's least significant 0's
			int tot = 0;
			REP(i, HSH) {
				tot += __builtin_ctz(~mx[i]);
			}
			return pow(2.0, tot * 1.0 / HSH) / 0.77351;
		}
	};

/*
	FM fm[MAXNODE];
	int approx_value[MAXNODE];
	void FM_Sketch() {
		for (int i=1; i<=n; i++){
			fm[i].init();
			for (int j=0; j<g[i].size(); j++){
				int eid = g[i][j];
				fm[i].insert(eid);
			}
			//if (g[i].size() >=75){
				//printf(" %d\t%.2lf\t%.0lf\t%.2lf\n", g[i].size(), fm[i].count1(), fm[i].count2(), fm[i].count3());
			//}else{
				//if (fm[i].count3()>=40){
					//printf("%d\t%.2lf\t%.0lf\t%.2lf\n", g[i].size(), fm[i].count1(), fm[i].count2(), fm[i].count3());
				//}
			//}
		}
	}
*/

#endif /* EFFICIENTTOPK_H_ */
