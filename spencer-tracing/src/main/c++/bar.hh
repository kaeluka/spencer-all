#ifndef BAR_H
#define BAR_H

void bar(const string &name, long num, long maximum, std::ostream &str) {
  if (maximum <= 0) {
    return;
  }

  //printf(" (%15ld) ", num);

  int next_power_of_ten = (int)pow(10.0, ceil(log10((double)maximum)));
  const long BARLENGTH = (num * 70 / next_power_of_ten);
  for (int i = 0; i < BARLENGTH; ++i) {
    str << "#";
  }
  str << " "<<num<<" "<<name;
  str << "\n";
}

#endif /* end of include guard: BAR_H */
