#pragma once
class Row_vll {
 public:
  void init(row_t* row);
  // return true   : the access is blocked.
  // return false	 : the access is NOT blocked
  bool insert_access(access_t type);
  void remove_access(access_t type);
  int get_cs() { return cs; };

 private:
  row_t* _row;
  int cs;
  int cx;
};
