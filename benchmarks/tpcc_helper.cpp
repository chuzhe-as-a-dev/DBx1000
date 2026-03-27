#include "tpcc_helper.h"

drand48_data ** tpcc_buffer;

uint64_t distKey(uint64_t d_id, uint64_t d_w_id)  {
	return d_w_id * DIST_PER_WARE + d_id; 
}

uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
	return (distKey(c_d_id, c_w_id) * g_cust_per_dist + c_id);
}

uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return distKey(d_id, w_id) * g_cust_per_dist + o_id; 
}

uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return orderlineKey(w_id, d_id, o_id); 
}

// Encodes a customer's last name + district + warehouse into a single 64-bit
// index key. Each character is mapped to an offset from 'A' (2 bits), packed
// left-to-right with 2-bit shifts. The low 3 bits are reserved for
// the district/warehouse composite to keep the key unique across districts. (AI-generated)
uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id) {
	uint64_t key = 0;
	char offset = 'A';
	for (uint32_t i = 0; i < strlen(c_last); i++) 
		key = (key << 2) + (c_last[i] - offset);
	key = key << 3;
	key += c_w_id * DIST_PER_WARE + c_d_id;
	return key;
}

uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id) {
	return s_w_id * g_max_items + s_i_id;
}

// Generates a TPC-C customer last name from a number in [0, 999].
// The three decimal digits of num each independently select one of 10
// fixed syllables (hundreds, tens, units), producing 1000 distinct names. (AI-generated)
uint64_t Lastname(uint64_t num, char* name) {
  	static const char *n[] =
    	{"BAR", "OUGHT", "ABLE", "PRI", "PRES",
     	"ESE", "ANTI", "CALLY", "ATION", "EING"};
  	strcpy(name, n[num/100]);
  	strcat(name, n[(num/10)%10]);
  	strcat(name, n[num%10]);
  	return strlen(name);
}

uint64_t RAND(uint64_t max, uint64_t thd_id) {
	int64_t rint64 = 0;
	lrand48_r(tpcc_buffer[thd_id], &rint64);
	return rint64 % max;
}

uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
    return x + RAND(y - x + 1, thd_id);
}

// TPC-C non-uniform random: ((URand(0,A) | URand(x,y)) + C) % (y-x+1) + x.
// C is a per-A constant fixed on first use; C++11 guarantees function-local
// static initialization is thread-safe and occurs exactly once.
// A must be one of {255, 1023, 8191} (used for NURand of customer/item IDs). (AI-generated)
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
  static uint64_t C_255  = URand(0, 255,  thd_id);
  static uint64_t C_1023 = URand(0, 1023, thd_id);
  static uint64_t C_8191 = URand(0, 8191, thd_id);
  uint64_t C;
  switch (A) {
    case 255:  C = C_255;  break;
    case 1023: C = C_1023; break;
    case 8191: C = C_8191; break;
    default:
      M_ASSERT(false, "Error! NURand\n");
      exit(-1);
  }
  return (((URand(0, A, thd_id) | URand(x, y, thd_id)) + C) % (y - x + 1)) + x;
}

uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id) {
    char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
    uint64_t cnt = URand(min, max, thd_id);
    for (uint32_t i = 0; i < cnt; i++) 
		str[i] = char_list[URand(0L, 60L, thd_id)];
    for (int i = cnt; i < max; i++)
		str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id) {

  uint64_t cnt = URand(min, max, thd_id);
  for (UInt32 i = 0; i < cnt; i++) {
    uint64_t r = URand(0L,9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}

uint64_t wh_to_part(uint64_t wid) {
	assert(g_part_cnt <= g_num_wh);
	return wid % g_part_cnt;
}
