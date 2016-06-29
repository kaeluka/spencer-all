extern long global_event_counter;

/// Counts the number of events the object was alive as STOP -
/// START where STOP and START denote the first respective last
/// time the object was the subject of an event
struct proto_global_lifetime {
  string cname = "";
  long first = 0;
  long last = 0;

  void fail() const { }

  static string description() { return __FILE__; }

  inline bool allocate(long, const char *cname, const char *thread) {
    if (this->cname == "") this->cname = string(cname);
    ++global_event_counter;
    if (this->first == 0) this->first = global_event_counter;
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped, long holder, const char *cname, const char *fname, const char *thread) {
    ++global_event_counter;
    this->last = global_event_counter;
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname, const char *thread) {
    ++global_event_counter;
    this->last = global_event_counter;
    return true;
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var, const char *cname, const char *mname, const char *thread) {
    ++global_event_counter;
    this->last = global_event_counter;
    return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname, int var, const char *thread) {
    ++global_event_counter;
    this->last = global_event_counter;
    return true;
  }

  inline bool useFrom(bool isModify, long obj, const char *cname, const char *mname, const char *fname, const char *thread) {
    ++global_event_counter;
    this->last = global_event_counter;
    return true;
  }

  static string classSummary() {
    return "";
  }

  string resultAsString() const {
    return std::to_string(this->last - this->first);
  }
};
