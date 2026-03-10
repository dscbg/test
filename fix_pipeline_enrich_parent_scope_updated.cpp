#ifndef USE_GOOGLE_LOG
#define USE_GOOGLE_LOG
#endif

#include <algorithm>
#include <cctype>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

// =========================
// Shared helpers
// =========================

static void ensureDir(const std::string& dir) { mkdir(dir.c_str(), 0777); }

// Convert FIX UTC timestamp (YYYYMMDD-HH:MM:SS[.sss]) to EST by fixed -5h.
// NOTE: does NOT handle daylight saving time.
static std::string utcToEstFixed5(const std::string& utcStr) {
    if (utcStr.empty()) return "";

    std::string baseTime = utcStr;
    std::string msPart = "";
    size_t dotPos = utcStr.find('.');
    if (dotPos != std::string::npos) {
        baseTime = utcStr.substr(0, dotPos);
        msPart = utcStr.substr(dotPos);
    }

    struct tm tmUTC;
    std::memset(&tmUTC, 0, sizeof(tmUTC));
    if (strptime(baseTime.c_str(), "%Y%m%d-%H:%M:%S", &tmUTC) == NULL) {
        return utcStr;
    }

    time_t utcTime = timegm(&tmUTC);
    time_t estTime = utcTime - (5 * 3600);

    struct tm* estTm = gmtime(&estTime);
    char buffer[32];
    std::memset(buffer, 0, sizeof(buffer));
    strftime(buffer, sizeof(buffer), "%Y%m%d-%H:%M:%S", estTm);
    return std::string(buffer) + msPart;
}

static std::string trim(const std::string& in) {
    size_t start = in.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = in.find_last_not_of(" \t\n\r");
    return in.substr(start, end - start + 1);
}

static bool iequals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower((unsigned char)a[i]) != std::tolower((unsigned char)b[i])) return false;
    }
    return true;
}

static inline bool endsWith(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static std::string getServerNum(char firstChar) {
    switch (firstChar) {
        case 'C': return "3";
        case 'D': return "8";
        case 'F': return "6";
        case 'E': return "5";
        default:  return "unknown";
    }
}

static std::vector<std::string> splitFixMessages(const std::string& line) {
    std::vector<std::string> messages;
    const std::string delimiter = "8=FIX";
    size_t pos = line.find(delimiter);

    while (pos != std::string::npos) {
        size_t nextPos = line.find(delimiter, pos + delimiter.length());
        if (nextPos == std::string::npos) messages.push_back(line.substr(pos));
        else messages.push_back(line.substr(pos, nextPos - pos));
        pos = nextPos;
    }
    return messages;
}

static std::string getTag(const std::string& line, const std::string& tag) {
    std::string s1 = " " + tag + "=";
    std::string s2 = "\001" + tag + "=";
    std::string s3 = "^A" + tag + "=";

    size_t pos = line.find(s1);
    if (pos == std::string::npos) pos = line.find(s2);
    if (pos == std::string::npos) pos = line.find(s3);
    if (pos == std::string::npos) return "";

    size_t valStart = line.find('=', pos);
    if (valStart == std::string::npos) return "";
    valStart += 1;

    size_t valEnd = line.find_first_of(" \001^", valStart);
    if (valEnd == std::string::npos) valEnd = line.size();
    return line.substr(valStart, valEnd - valStart);
}

static std::string extractTextValueColon(const std::string& line, const std::string& key) {
    std::string token = key + ":";
    size_t pos = line.find(token);
    if (pos == std::string::npos) return "";

    size_t start = pos + token.size();
    while (start < line.size() && (line[start] == ' ' || line[start] == '\t')) ++start;
    if (start >= line.size()) return "";

    size_t end = start;
    while (end < line.size()) {
        char c = line[end];
        if (c == '[' || c == '\t' || c == '\r' || c == '\n' || c == ',' || c == '^' || c == '\001') break;
        ++end;
    }
    if (end <= start) return "";
    return trim(line.substr(start, end - start));
}

static std::string extractTextValueSpace(const std::string& line, const std::string& key) {
    std::string token = key + " ";
    size_t pos = line.find(token);
    if (pos == std::string::npos) return "";

    size_t start = pos + token.size();
    while (start < line.size() && (line[start] == ' ' || line[start] == '\t')) ++start;
    if (start >= line.size()) return "";

    size_t end = start;
    while (end < line.size()) {
        char c = line[end];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == ',' || c == '^' || c == '\001') break;
        ++end;
    }
    if (end <= start) return "";
    return trim(line.substr(start, end - start));
}

static std::string extractTextValueFlexible(const std::string& line, const std::string& key, bool allowSpace) {
    std::string v = extractTextValueColon(line, key);
    if (!v.empty()) return v;
    if (!allowSpace) return "";
    return extractTextValueSpace(line, key);
}

static std::string lineExcerpt(const std::string& line) {
    const size_t kMax = 220;
    if (line.size() <= kMax) return line;
    return line.substr(0, kMax);
}

static bool execIdMatchesXcid(const std::string& inputExecIdRaw, const std::string& xcidRaw) {
    return iequals(trim(inputExecIdRaw), trim(xcidRaw));
}

static std::string normalizeCharToAsciiIfSingle(const std::string& v) {
    std::string t = trim(v);
    if (t.size() == 1) {
        unsigned char c = (unsigned char)t[0];
        return std::to_string((int)c);
    }
    return t;
}

static std::vector<std::string> parseCsvLine(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    bool inQuotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == '"') {
            if (inQuotes && i + 1 < line.size() && line[i + 1] == '"') {
                cur.push_back('"');
                ++i;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (c == ',' && !inQuotes) {
            out.push_back(trim(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(trim(cur));
    return out;
}

static std::string csvEscape(const std::string& v) {
    if (v.find(',') == std::string::npos && v.find('"') == std::string::npos && v.find('\n') == std::string::npos) {
        return v;
    }
    std::string out = "\"";
    for (size_t i = 0; i < v.size(); ++i) {
        if (v[i] == '"') out += "\"\"";
        else out.push_back(v[i]);
    }
    out += "\"";
    return out;
}

static const std::unordered_set<std::string> kDropOriginalColumns = {
    "File"
};

static const std::unordered_map<std::string, std::string> kRenameOriginalColumns = {
    {"Exec_ID", "exchange_exec_id"},
    {"LastQty","filled_size_delta"},
    {"Status","status"},
    {"ClOrdID","clorderid"},
    {"Sym","sym"},
    {"Exchange_ID","exch_id"}
};

static const std::unordered_set<std::string> kOriginalIntColumns = {
    "LastQty",
    "CumQty",
    "OrderQty",
};

static const std::vector<std::string> kExtraEmptyColumns = {
};

static const std::vector<std::string> kOutputColumnOrder = {
    "date", "sym", "time", "sor_id", "side", "next_index", "status", "action",
    "prev_index", "algo_id", "cid", "children", "target_size", "uncommitted_size",
    "filled_size", "last_filled_price", "filled_notional", "committed_size",
    "onleave_internal", "onleave_market", "id", "update_time", "update_time_micros",
    "exchange_transact_time", "creation_time", "index", "price", "exch_id",
    "parent", "original", "algo_sender_compid", "order_id_by_algo", "cross_type",
    "tif", "quote_size", "quote_type", "clorderid", "causal_msg_seqnum",
    "quote_time", "wave_index", "seq_index", "bsor_exec_id", "exchange_exec_id",
    "bid_exch_id", "bid_nbbo_price", "bid_ubbo_price0", "bid_ubbo_price1",
    "bid_ubbo_price2", "bid_nbbo_size", "bid_ubbo_size0", "bid_ubbo_size1",
    "bid_ubbo_size2", "bid_nbbo_time", "bid_ubbo_time0", "bid_ubbo_time1",
    "bid_ubbo_time2", "ask_exch_id", "ask_nbbo_price", "ask_ubbo_price0",
    "ask_ubbo_price1", "ask_ubbo_price2", "ask_nbbo_size", "ask_ubbo_size0",
    "ask_ubbo_size1", "ask_ubbo_size2", "ask_nbbo_time", "ask_ubbo_time0",
    "ask_ubbo_time1", "ask_ubbo_time2", "max_floor", "quote_size_to_limit_px",
    "min_fill_size", "exec_inst", "reserved", "broker_id", "filled_size_delta",
    "liquidity_indicator"
};

static std::string castOriginalColumnValue(const std::string& name, const std::string& v) {
    if(name == "status") return normalizeCharToAsciiIfSingle(v);
    return v;
}

static inline std::string defaultOriginalMissing(const std::string& headerName) {
    return kOriginalIntColumns.count(headerName) ? "0" : "";
}

enum SourceType { TEXT_KV, FIX_TAG, DUAL_TEXT_TAG, COMPUTED };
enum ValueType { VT_STRING, VT_INT };
enum MatchScope { MS_CLORD_ONLY, MS_PAIR_STRICT, MS_PARENT_TEXT, MS_PARENT_FIX };

typedef std::string (*NormalizeFn)(const std::string&);
typedef std::string (*ComputeFn)(const std::unordered_map<std::string, std::string>& textKv,
                                 const std::unordered_map<std::string, std::string>& fixTags,
                                 const std::string& sourceLine);

static std::unordered_map<std::string, ValueType> gEnrichedColTypes;

static bool linePrefilter(const std::string& line) {
    if (line.find("8=FIX") != std::string::npos) return true;
    if (line.find("strtID:") != std::string::npos || line.find("strtID ") != std::string::npos) return true;
    if (line.find("xcid:") != std::string::npos || line.find("xcid ") != std::string::npos) return true;
    if (line.find("id:") != std::string::npos && line.find("strt:0") != std::string::npos) return true;
    if (line.find("11=") != std::string::npos) return true;
    if (line.find("9607=") != std::string::npos) return true;
    if (line.find("17=") != std::string::npos) return true;
    return false;
}

static void processAllLogs(const std::string& directory, const std::string& outputCsv) {
    DIR* dir = opendir(directory.c_str());
    if (!dir) {
        std::cerr << "Cannot open directory: " << directory << "\n";
        return;
    }

    ensureDir("search_results");
    std::ofstream csv(outputCsv.c_str(), std::ios::app);

    std::ifstream checkEmpty(outputCsv.c_str());
    bool isNew = (checkEmpty.peek() == std::ifstream::traits_type::eof());
    checkEmpty.close();

    if (isNew && csv.is_open()) {
        csv << "ClOrdID,Exchange_ID,Exec_ID,LastQty,File,Sym,Status\n";
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        std::string filename = entry->d_name;
        if (filename.empty() || filename[0] == '.' || endsWith(filename, ".gz")) continue;

        std::string fullPath = directory + "/" + filename;
        std::ifstream file(fullPath.c_str());
        if (!file.is_open()) continue;

        std::string line;
        while (std::getline(file, line)) {
            std::vector<std::string> messages = splitFixMessages(line);
            for (size_t i = 0; i < messages.size(); ++i) {
                const std::string& msg = messages[i];
                std::string execType = getTag(msg, "150");

                if (execType == "1" || execType == "2" || execType == "F") {
                    std::string raw49 = getTag(msg, "49");
                    if (raw49.find("ABOVE") == 0 || raw49.find("ABDGW") == 0) continue;

                    std::string lastQtyStr = getTag(msg, "32");
                    int lastQty = 0;
                    if (!lastQtyStr.empty()) {
                        try { lastQty = std::stoi(lastQtyStr); } catch (...) { lastQty = 0; }
                    }
                    if (lastQty <= 0) continue;

                    std::string exchangeId = getTag(msg, "30");
                    if (exchangeId.empty()) exchangeId = raw49;

                    std::string execId = getTag(msg, "9607");
                    if (execId.empty()) execId = getTag(msg, "17");

                    if (csv.is_open()) {
                        csv << getTag(msg, "11") << ","
                            << (exchangeId.empty() ? "UNKNOWN" : exchangeId) << ","
                            << (execId.empty() ? "N/A" : execId) << ","
                            << lastQty << ","
                            << fullPath << ","
                            << getTag(msg, "55") << ","
                            << execType << "\n";
                    }
                }
            }
        }
    }

    if (csv.is_open()) csv.close();
    closedir(dir);
}

static std::string normalizeTrim(const std::string& v) { return trim(v); }

static std::string normalizeHexToDec(const std::string& v) {
    std::string t = trim(v);
    if (t.empty()) return "";
    unsigned long long value = 0;
    std::stringstream ss(t);
    ss >> std::hex >> value;
    if (ss.fail()) return "";
    return std::to_string(value);
}

struct ExtractorSpec {
    std::string field_name;
    SourceType source_type;
    std::string text_key;
    std::string fix_tag;
    bool allow_space;
    std::string out_final_col;
    std::string out_mismatch_col;
    NormalizeFn normalize_text;
    NormalizeFn normalize_tag;
    ComputeFn compute;
    ValueType value_type;
    MatchScope match_scope;
};

static inline std::string defaultMissing(ValueType) {
    return "";
}

static ExtractorSpec Text(const std::string& outCol,
                          const std::string& textKey,
                          NormalizeFn norm = normalizeTrim,
                          bool allowSpace = false,
                          MatchScope scope = MS_CLORD_ONLY) {
    ExtractorSpec s;
    s.field_name = outCol;
    s.source_type = TEXT_KV;
    s.text_key = textKey;
    s.fix_tag = "";
    s.allow_space = allowSpace;
    s.out_final_col = outCol;
    s.out_mismatch_col = "";
    s.normalize_text = norm;
    s.normalize_tag = normalizeTrim;
    s.compute = NULL;
    s.value_type = VT_STRING;
    s.match_scope = scope;
    return s;
}

static ExtractorSpec Tag(const std::string& outCol,
                         const std::string& fixTag,
                         NormalizeFn norm = normalizeTrim,
                         MatchScope scope = MS_PAIR_STRICT) {
    ExtractorSpec s;
    s.field_name = outCol;
    s.source_type = FIX_TAG;
    s.text_key = "";
    s.fix_tag = fixTag;
    s.allow_space = false;
    s.out_final_col = outCol;
    s.out_mismatch_col = "";
    s.normalize_text = normalizeTrim;
    s.normalize_tag = norm;
    s.compute = NULL;
    s.value_type = VT_STRING;
    s.match_scope = scope;
    return s;
}

static ExtractorSpec Dual(const std::string& outCol,
                          const std::string& textKey,
                          const std::string& fixTag,
                          NormalizeFn normText = normalizeTrim,
                          NormalizeFn normTag = normalizeTrim,
                          bool allowSpace = false,
                          MatchScope scope = MS_CLORD_ONLY) {
    ExtractorSpec s;
    s.field_name = outCol;
    s.source_type = DUAL_TEXT_TAG;
    s.text_key = textKey;
    s.fix_tag = fixTag;
    s.allow_space = allowSpace;
    s.out_final_col = outCol;
    s.out_mismatch_col = outCol + "_mismatch";
    s.normalize_text = normText;
    s.normalize_tag = normTag;
    s.compute = NULL;
    s.value_type = VT_STRING;
    s.match_scope = scope;
    return s;
}

static ExtractorSpec Computed(const std::string& outCol, ComputeFn fn, MatchScope scope = MS_PAIR_STRICT) {
    ExtractorSpec s;
    s.field_name = outCol;
    s.source_type = COMPUTED;
    s.text_key = "";
    s.fix_tag = "";
    s.allow_space = false;
    s.out_final_col = outCol;
    s.out_mismatch_col = "";
    s.normalize_text = NULL;
    s.normalize_tag = NULL;
    s.compute = fn;
    s.value_type = VT_STRING;
    s.match_scope = scope;
    return s;
}

static std::string computeLiquidity(const std::unordered_map<std::string, std::string>&,
                                    const std::unordered_map<std::string, std::string>&,
                                    const std::string& line) {
    std::string t851 = getTag(line, "851");
    if (!t851.empty()) return normalizeCharToAsciiIfSingle(t851);
    std::string t9730 = getTag(line, "9730");
    if(t9730 == "R") return "50";
    if(t9730 == "A") return "49";
    if(!t9730.empty()) return normalizeCharToAsciiIfSingle(t9730);
    if (line.find("59=3") != std::string::npos) return "50";
    if (line.find("tif:IOC") != std::string::npos) return "50";
    return "UNKNOWN";
}

static std::string computeSendingTimeEstFull(const std::unordered_map<std::string, std::string>&,
                                             const std::unordered_map<std::string, std::string>&,
                                             const std::string& line) {
    std::string t60 = getTag(line, "60");
    if (t60.empty()) return "";
    return utcToEstFixed5(trim(t60));
}

static std::string computeSendingDateEst(const std::unordered_map<std::string, std::string>& textKv,
                                         const std::unordered_map<std::string, std::string>& fixTags,
                                         const std::string& line) {
    (void)textKv; (void)fixTags;
    std::string est = computeSendingTimeEstFull(textKv, fixTags, line);
    if (est.size() < 8) return "UNKNOWN";
    return est.substr(0, 8);
}

static std::string computeSendingTimeEst(const std::unordered_map<std::string, std::string>&,
                                         const std::unordered_map<std::string, std::string>&,
                                         const std::string& line) {
    std::string est = extractTextValueColon(line, "INFO");
    if (est.empty()) return "UNKNOWN";
    return est;
}

static std::string computeSendingTimeEstMilli(const std::unordered_map<std::string, std::string>& textKv,
                                              const std::unordered_map<std::string, std::string>& fixTags,
                                              const std::string& line) {
    std::string est = computeSendingTimeEst(textKv, fixTags, line);
    if(est=="UNKOWN") return "";
    int hh=0, mm=0, ss=0, s_sub=0;
    sscanf(est.c_str(), "%d:%d:%d.%d", &hh, &mm, &ss, &s_sub);
    long long totalMillis = (hh * 3600000LL) + (mm * 60000LL) + (ss * 1000LL) + (s_sub / 1000);
    return std::to_string(totalMillis);
}

static std::string computeSendingTimeEstTime(const std::unordered_map<std::string, std::string>& textKv,
                                             const std::unordered_map<std::string, std::string>& fixTags,
                                             const std::string& line) {
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    if (est.length() >= 12) return est.substr(0, 12);
    return est;
}

static std::string computeSendingTimeEstMicro(const std::unordered_map<std::string, std::string>& textKv,
                                              const std::unordered_map<std::string, std::string>& fixTags,
                                              const std::string& line) {
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    int hh=0, mm=0, ss=0, fractional=0;
    if (sscanf(est.c_str(), "%d:%d:%d.%d", &hh, &mm, &ss, &fractional) == 4) {
        int microRemainder = fractional % 1000;
        return std::to_string(microRemainder);
    }
    return "";
}

static std::string computeSendingTimeEstTransact(const std::unordered_map<std::string, std::string>& textKv,
                                                 const std::unordered_map<std::string, std::string>& fixTags,
                                                 const std::string& line) {
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    int hh=0, mm=0, ss=0, fractional=0;
    if (sscanf(est.c_str(), "%d:%d:%d.%d", &hh, &mm, &ss, &fractional) == 4) {
        long long baseMillis = (hh * 3600000LL) + (mm * 60000LL) + (ss * 1000LL);
        int millisPart = fractional / 1000;
        int microRemainder = fractional % 1000;
        long long roundedMillis = baseMillis + millisPart;
        if (microRemainder >= 500) roundedMillis += 1;
        long long finalMicros = roundedMillis * 1000LL;
        return std::to_string(finalMicros);
    }
    return "";
}

static std::string diagnoseAlgoSenderCompid(const std::unordered_map<std::string, std::string>& fixTags,
                                            const std::string& line) {
    std::string t49;
    std::unordered_map<std::string, std::string>::const_iterator it49 = fixTags.find("49");
    if (it49 != fixTags.end()) t49 = trim(it49->second);
    if (t49.empty()) t49 = getTag(line, "49");
    if (t49.empty()) return "missing_tag49";

    if (t49.compare(0, 4, "CASH") == 0) {
        std::string t50;
        std::unordered_map<std::string, std::string>::const_iterator it50 = fixTags.find("50");
        if (it50 != fixTags.end()) t50 = trim(it50->second);
        if (t50.empty()) t50 = getTag(line, "50");
        if (t50.empty()) return "missing_tag50_for_cash_sender";
        if (t50.size() < 3) return "tag50_shorter_than_3";
        return "";
    }

    std::string t9702;
    std::unordered_map<std::string, std::string>::const_iterator it9702 = fixTags.find("9702");
    if (it9702 != fixTags.end()) t9702 = trim(it9702->second);
    if (t9702.empty()) t9702 = getTag(line, "9702");
    if (t9702.empty()) return "missing_tag9702";
    return "";
}

static std::string computeAlgoSenderId(const std::unordered_map<std::string, std::string>&,
                                       const std::unordered_map<std::string, std::string>& fixTags,
                                       const std::string& line) {
    std::string t49;
    std::unordered_map<std::string, std::string>::const_iterator it49 = fixTags.find("49");
    if (it49 != fixTags.end()) t49 = trim(it49->second);
    if (t49.empty()) t49 = getTag(line, "49");
    if (t49.empty()) return "";

    if (t49.compare(0, 4, "CASH") == 0) {
        std::string t50;
        std::unordered_map<std::string, std::string>::const_iterator it50 = fixTags.find("50");
        if (it50 != fixTags.end()) t50 = trim(it50->second);
        if (t50.empty()) t50 = getTag(line, "50");
        if (t50.empty() || t50.size() < 3) return "";
        return t49 + " " + t50.substr(0, 3);
    }

    std::string t9702;
    std::unordered_map<std::string, std::string>::const_iterator it9702 = fixTags.find("9702");
    if (it9702 != fixTags.end()) t9702 = trim(it9702->second);
    if (t9702.empty()) t9702 = getTag(line, "9702");
    if (t9702.empty()) return "";
    return t49 + " " + t9702;
}

static std::string trimToMillis(const std::string& s) {
    std::string t = trim(s);
    size_t dot = t.find('.');
    if (dot == std::string::npos) return t;
    if (dot + 4 <= t.size()) return t.substr(0, dot + 4);
    return t;
}

static std::string computeCreationTime(const std::unordered_map<std::string, std::string>&,
                                       const std::unordered_map<std::string, std::string>& fixTags,
                                       const std::string& line) {
    std::string mType;
    std::unordered_map<std::string, std::string>::const_iterator it35 = fixTags.find("35");
    if (it35 != fixTags.end()) mType = trim(it35->second);
    if (mType.empty()) mType = getTag(line, "35");
    if (mType != "D" && mType != "G") return "";

    std::string infoTs = extractTextValueColon(line, "INFO");
    if (infoTs.empty()) return "";
    return trimToMillis(infoTs);
}

static std::string computeNotional(const std::unordered_map<std::string, std::string>&,
                                   const std::unordered_map<std::string, std::string>&,
                                   const std::string& line) {
    std::string t6 = getTag(line, "6");
    std::string t14 = getTag(line,"14");
    if(t6.empty() || t14.empty()) return "";
    return std::to_string(std::stod(t6) * std::stod(t14));
}

static std::vector<ExtractorSpec> buildExtractorSpecs() {
    std::vector<ExtractorSpec> specs;

    specs.push_back(Text("next_index", "nxt")); specs.back().value_type = VT_INT;
    specs.push_back(Text("action", "act")); specs.back().value_type = VT_STRING;
    specs.push_back(Text("wave_index", "wav")); specs.back().value_type = VT_INT;
    specs.push_back(Text("prev_index", "prv")); specs.back().value_type = VT_INT;
    specs.push_back(Text("quote_size", "qsz")); specs.back().value_type = VT_INT;
    specs.push_back(Text("quote_size_to_limit_px", "qsz")); specs.back().value_type = VT_INT;
    specs.push_back(Text("quote_type", "qtyp")); specs.back().value_type = VT_STRING;
    specs.push_back(Text("seq_index", "seq")); specs.back().value_type = VT_INT;

    specs.push_back(Text("id", "id", normalizeHexToDec)); specs.back().value_type = VT_INT;
    specs.push_back(Text("original", "ori"));
    specs.push_back(Text("parent", "prnt"));
    specs.push_back(Text("min_fill_size", "mqty")); specs.back().value_type = VT_INT;
    specs.push_back(Text("committed_size", "cmtd")); specs.back().value_type = VT_INT;
    specs.push_back(Text("children", "chld")); specs.back().value_type = VT_INT;
    specs.push_back(Text("uncommitted_size", "ncmtd")); specs.back().value_type = VT_INT;
    specs.push_back(Text("last_market", "lskmkt")); specs.back().value_type = VT_INT;

    specs.push_back(Text("index", "order index", normalizeTrim, true)); specs.back().value_type = VT_INT;
    specs.push_back(Text("last_filled_price", "lastPx"));

    specs.push_back(Text("algo_id", "aid", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;
    specs.push_back(Text("onleave_internal", "oitn", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;
    specs.push_back(Text("onleave_market", "omkt", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;

    specs.push_back(Tag("causal_msg_seqnum", "34")); specs.back().value_type = VT_INT;
    specs.push_back(Tag("tif", "59", normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT;

    specs.push_back(Dual("side", "sd", "54", normalizeCharToAsciiIfSingle, normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_STRING;
    specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("price", "px", "44")); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("filled_size", "fll", "14")); specs.back().value_type = VT_INT; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("exec_inst", "t18", "18", normalizeTrim, normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("broker_id", "brkr", "76")); specs.back().value_type = VT_INT; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("target_size", "sz", "38")); specs.back().value_type = VT_INT; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Dual("cross_type", "aggr", "40", normalizeCharToAsciiIfSingle, normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT; specs.back().match_scope = MS_PAIR_STRICT;

    specs.push_back(Computed("filled_notional", &computeNotional)); specs.back().value_type = VT_STRING;
    specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("liquidity_indicator", &computeLiquidity)); specs.back().value_type = VT_STRING;
    specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("date", &computeSendingDateEst)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("update_time", &computeSendingTimeEstMilli)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("update_time_micros", &computeSendingTimeEstMicro)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("time", &computeSendingTimeEstTime)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("exchange_transact_time", &computeSendingTimeEstTransact)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_PAIR_STRICT;
    specs.push_back(Computed("creation_time", &computeCreationTime)); specs.back().value_type = VT_STRING; specs.back().match_scope = MS_CLORD_ONLY;

    specs.push_back(Computed("algo_sender_compid", &computeAlgoSenderId, MS_PARENT_FIX)); specs.back().value_type = VT_STRING;

    return specs;
}

struct DiffRow {
    std::vector<std::string> cols;
    std::string clOrdId;
    std::string execId;
    std::string filePath;
    std::string parentId;
};

struct ErrorRow {
    std::string clOrdId;
    std::string execId;
    std::string field;
    std::string textValue;
    std::string tagValue;
    std::string file;
    std::string excerpt;
};

struct ParentDebugState {
    bool sawParentTextLine;
    bool sawParentFixLine;
    std::string parentTextFile;
    std::string parentFixFile;
    std::string parentTextExcerpt;
    std::string parentFixExcerpt;
    std::string algoSenderMissingReason;

    ParentDebugState()
        : sawParentTextLine(false), sawParentFixLine(false) {}
};

struct TargetState {
    DiffRow row;
    std::unordered_map<std::string, std::string> outValues;
    std::unordered_set<std::string> mismatchRecorded;
    bool matched;
    ParentDebugState parentDebug;
};

static std::string unknownValue() { return "UNKNOWN"; }

static void setIfUnknown(std::unordered_map<std::string, std::string>& out,
                         const std::string& key,
                         const std::string& value) {
    if (key.empty()) return;
    std::string normalized = trim(value);
    if (normalized.empty()) return;

    std::unordered_map<std::string, std::string>::iterator it = out.find(key);
    if (it == out.end() || it->second.empty() || it->second == unknownValue()) {
        out[key] = normalized;
        return;
    }

    if (trim(it->second) != normalized) {
        LOG(WARNING) << "Keeping existing value for " << key
                     << " existing='" << it->second << "' new='" << normalized << "'";
    }
}

static void applyDualField(const ExtractorSpec& spec,
                           const std::string& maybeTextRaw,
                           const std::string& maybeTagRaw,
                           TargetState& target,
                           std::vector<ErrorRow>&,
                           const std::string&,
                           const std::string&) {
    std::string textVal = "";
    std::string tagVal = "";

    if (!maybeTextRaw.empty()) {
        NormalizeFn nt = spec.normalize_text ? spec.normalize_text : normalizeTrim;
        textVal = nt ? nt(maybeTextRaw) : maybeTextRaw;
    }
    if (!maybeTagRaw.empty()) {
        NormalizeFn nt = spec.normalize_tag ? spec.normalize_tag : normalizeTrim;
        tagVal = nt ? nt(maybeTagRaw) : maybeTagRaw;
    }

    bool hasText = !trim(textVal).empty();
    bool hasTag  = !trim(tagVal).empty();
    if (!hasText && !hasTag) return;

    std::unordered_map<std::string, std::string>::const_iterator itExisting =
        target.outValues.find(spec.out_final_col);
    std::string existing = (itExisting == target.outValues.end()) ? "" : trim(itExisting->second);
    bool hasExisting = !existing.empty() && existing != unknownValue();

    std::string chosen = "";
    if (hasText) chosen = textVal;
    else if (hasTag) chosen = tagVal;

    if (!hasExisting) {
        if (!chosen.empty()) target.outValues[spec.out_final_col] = chosen;
        return;
    }

    if (!chosen.empty() && existing != chosen) {
        LOG(WARNING) << "Keeping existing value for " << spec.out_final_col
                     << " ClOrdID=" << target.row.clOrdId
                     << " ExecID=" << target.row.execId
                     << " existing='" << existing << "' new='" << chosen << "'";
    }
}

static void applyExtractorSpecs(const std::vector<ExtractorSpec>& specs,
                                const std::unordered_map<std::string, std::string>& textKv,
                                const std::unordered_map<std::string, std::string>& fixTags,
                                TargetState& target,
                                std::vector<ErrorRow>& errors,
                                const std::string& sourceFile,
                                const std::string& sourceLine,
                                MatchScope activeScope) {
    for (size_t i = 0; i < specs.size(); ++i) {
        const ExtractorSpec& spec = specs[i];
        if (spec.match_scope != activeScope) continue;

        if (spec.source_type == TEXT_KV) {
            std::unordered_map<std::string, std::string>::const_iterator it = textKv.find(spec.text_key);
            if (it != textKv.end()) {
                NormalizeFn nt = spec.normalize_text ? spec.normalize_text : normalizeTrim;
                std::string v = nt ? nt(it->second) : it->second;
                if (!v.empty()) setIfUnknown(target.outValues, spec.out_final_col, v);
            }
        } else if (spec.source_type == FIX_TAG) {
            std::unordered_map<std::string, std::string>::const_iterator it = fixTags.find(spec.fix_tag);
            if (it != fixTags.end()) {
                NormalizeFn nt = spec.normalize_tag ? spec.normalize_tag : normalizeTrim;
                std::string v = nt ? nt(it->second) : it->second;
                if (!v.empty()) setIfUnknown(target.outValues, spec.out_final_col, v);
            }
        } else if (spec.source_type == DUAL_TEXT_TAG) {
            std::string rawText = "";
            std::string rawTag  = "";

            std::unordered_map<std::string, std::string>::const_iterator itText = textKv.find(spec.text_key);
            if (itText != textKv.end()) rawText = itText->second;
            std::unordered_map<std::string, std::string>::const_iterator itTag = fixTags.find(spec.fix_tag);
            if (itTag != fixTags.end()) rawTag = itTag->second;

            applyDualField(spec, rawText, rawTag, target, errors, sourceFile, sourceLine);
        } else if (spec.source_type == COMPUTED) {
            if (!spec.compute) continue;
            std::string v = spec.compute(textKv, fixTags, sourceLine);
            if (!v.empty()) setIfUnknown(target.outValues, spec.out_final_col, v);
        }
    }
}

static std::unordered_map<std::string, std::string> collectTextKVs(const std::string& line,
                                                                   const std::vector<ExtractorSpec>& specs) {
    std::unordered_map<std::string, std::string> kv;
    kv["strtID"] = extractTextValueFlexible(line, "strtID", true);
    kv["xcid"]   = extractTextValueFlexible(line, "xcid",   true);
    kv["id"]     = extractTextValueFlexible(line, "id",     false);

    for (size_t i = 0; i < specs.size(); ++i) {
        if (specs[i].source_type == TEXT_KV || specs[i].source_type == DUAL_TEXT_TAG) {
            if (!specs[i].text_key.empty()) {
                kv[specs[i].text_key] = extractTextValueFlexible(line, specs[i].text_key, specs[i].allow_space);
            }
        }
    }
    return kv;
}

static std::unordered_map<std::string, std::string> collectFixTags(const std::string& fixMsg,
                                                                   const std::vector<ExtractorSpec>& specs) {
    std::unordered_map<std::string, std::string> tags;
    tags["11"]   = getTag(fixMsg, "11");
    tags["9607"] = getTag(fixMsg, "9607");
    tags["17"]   = getTag(fixMsg, "17");
    tags["35"]   = getTag(fixMsg, "35");
    tags["49"]   = getTag(fixMsg, "49");
    tags["50"]   = getTag(fixMsg, "50");
    tags["60"]   = getTag(fixMsg, "60");
    tags["9702"] = getTag(fixMsg, "9702");

    for (size_t i = 0; i < specs.size(); ++i) {
        if (specs[i].source_type == FIX_TAG || specs[i].source_type == DUAL_TEXT_TAG) {
            if (!specs[i].fix_tag.empty()) tags[specs[i].fix_tag] = getTag(fixMsg, specs[i].fix_tag);
        }
    }
    return tags;
}

static bool isTargetConsistent(const TargetState& target,
                               const std::string& observedClOrd,
                               const std::string& observedExec,
                               bool observedExecIsXcid) {
    if (!observedClOrd.empty() && observedClOrd != target.row.clOrdId) return false;
    if (!observedExec.empty()) {
        if (observedExecIsXcid) {
            if (!execIdMatchesXcid(target.row.execId, observedExec)) return false;
        } else {
            if (!iequals(trim(target.row.execId), trim(observedExec))) return false;
        }
    }
    return true;
}

static void collectCandidatesByClOrd(const std::string& clOrd,
                                     const std::unordered_map<std::string, std::vector<int> >& byClOrd,
                                     std::unordered_set<int>& out) {
    if (clOrd.empty()) return;
    std::unordered_map<std::string, std::vector<int> >::const_iterator it = byClOrd.find(clOrd);
    if (it != byClOrd.end()) {
        for (size_t i = 0; i < it->second.size(); ++i) out.insert(it->second[i]);
    }
}

static void collectCandidatesByExec(const std::string& exec,
                                    const std::unordered_map<std::string, std::vector<int> >& byExec,
                                    std::unordered_set<int>& out) {
    if (exec.empty()) return;
    std::string key = trim(exec);
    std::unordered_map<std::string, std::vector<int> >::const_iterator it = byExec.find(key);
    if (it != byExec.end()) {
        for (size_t i = 0; i < it->second.size(); ++i) out.insert(it->second[i]);
    }
}

static size_t applyToCandidates(const std::unordered_set<int>& candidateIds,
                                std::vector<TargetState>& targets,
                                const std::string& observedClOrd,
                                const std::string& observedExec,
                                bool observedExecIsXcid,
                                const std::unordered_map<std::string, std::string>& textKv,
                                const std::unordered_map<std::string, std::string>& fixTags,
                                const std::vector<ExtractorSpec>& specs,
                                std::vector<ErrorRow>& errors,
                                const std::string& sourceFile,
                                const std::string& sourceLine) {
    size_t newlyMatched = 0;
    for (std::unordered_set<int>::const_iterator it = candidateIds.begin(); it != candidateIds.end(); ++it) {
        int idx = *it;
        if (idx < 0 || (size_t)idx >= targets.size()) continue;
        TargetState& target = targets[(size_t)idx];
        if (!isTargetConsistent(target, observedClOrd, observedExec, observedExecIsXcid)) continue;

        const bool hasClOrd = !observedClOrd.empty();
        const bool hasExec = !observedExec.empty();

        if (hasClOrd) applyExtractorSpecs(specs, textKv, fixTags, target, errors, sourceFile, sourceLine, MS_CLORD_ONLY);
        if (hasClOrd && hasExec) {
            applyExtractorSpecs(specs, textKv, fixTags, target, errors, sourceFile, sourceLine, MS_PAIR_STRICT);
            if (!target.matched) {
                target.matched = true;
                ++newlyMatched;
            }
        }
    }
    return newlyMatched;
}

static bool loadDiffCsv(const std::string& diffCsv,
                        std::vector<std::string>& headers,
                        std::vector<TargetState>& targets,
                        std::unordered_map<std::string, std::vector<int> >& byClOrd,
                        std::unordered_map<std::string, std::vector<int> >& byExec,
                        bool& hasFileColumn) {
    std::ifstream in(diffCsv.c_str());
    if (!in.is_open()) {
        LOG(ERROR) << "Failed to open diff CSV: " << diffCsv;
        return false;
    }

    std::string line;
    if (!std::getline(in, line)) {
        LOG(ERROR) << "Diff CSV is empty: " << diffCsv;
        return false;
    }

    headers = parseCsvLine(line);

    int clOrdIdx = -1;
    int execIdx = -1;
    int fileIdx = -1;

    for (size_t i = 0; i < headers.size(); ++i) {
        if (headers[i] == "ClOrdID") clOrdIdx = (int)i;
        if (headers[i] == "Exec_ID" || headers[i] == "ExecID") execIdx = (int)i;
        if (headers[i] == "File") fileIdx = (int)i;
    }

    hasFileColumn = (fileIdx >= 0);
    if (clOrdIdx < 0 || execIdx < 0) {
        LOG(ERROR) << "Diff CSV must contain ClOrdID and Exec_ID (or ExecID).";
        return false;
    }

    std::unordered_set<std::string> uniquePairs;
    ensureDir("search_results");
    std::string invalidPath = "search_results/invalid_diff_rows.csv";
    std::ofstream invalidOut(invalidPath.c_str(), std::ios::app);
    bool invalidNew = false;
    {
        std::ifstream chk(invalidPath.c_str());
        invalidNew = (!chk.is_open() || chk.peek() == std::ifstream::traits_type::eof());
    }
    if (invalidNew && invalidOut.is_open()) invalidOut << "LineNo,ClOrdID,ExecID,Reason,RowRaw\n";

    size_t lineNo = 1;
    while (std::getline(in, line)) {
        ++lineNo;
        if (trim(line).empty()) continue;

        std::vector<std::string> row = parseCsvLine(line);
        if ((int)row.size() <= std::max(clOrdIdx, execIdx)) continue;

        TargetState target;
        target.row.cols = row;
        target.row.clOrdId = trim(row[(size_t)clOrdIdx]);
        target.row.execId  = trim(row[(size_t)execIdx]);
        target.row.filePath = (fileIdx >= 0 && (int)row.size() > fileIdx) ? trim(row[(size_t)fileIdx]) : "";
        target.matched = false;

        if (target.row.clOrdId.size() != 14) {
            if (invalidOut.is_open()) {
                invalidOut << lineNo << "," << csvEscape(target.row.clOrdId) << ","
                           << csvEscape(target.row.execId) << ","
                           << "ClOrdID_length_not_14," << csvEscape(line) << "\n";
            }
            continue;
        }

        std::string pairKey = target.row.clOrdId + "\x1f" + target.row.execId;
        if (uniquePairs.count(pairKey)) continue;
        uniquePairs.insert(pairKey);

        int idx = (int)targets.size();
        targets.push_back(target);
        byClOrd[target.row.clOrdId].push_back(idx);
        byExec[target.row.execId].push_back(idx);
    }

    LOG(INFO) << "Loaded diff targets: " << targets.size();
    if (hasFileColumn) LOG(INFO) << "Diff CSV contains File column; per-row file scan enabled.";
    return true;
}

static bool loadParentMap(const std::string& mapCsv,
                          std::unordered_map<std::string, std::string>& clOrdToOrderIdByAlgo) {
    clOrdToOrderIdByAlgo.clear();
    if (mapCsv.empty()) return true;

    std::ifstream in(mapCsv.c_str());
    if (!in.is_open()) {
        LOG(ERROR) << "Failed to open order_id_by_algo map CSV: " << mapCsv;
        return false;
    }

    std::string line;
    if (!std::getline(in, line)) return true;

    std::vector<std::string> headers = parseCsvLine(line);
    int clIdx = -1;
    int pIdx = -1;
    for (size_t i = 0; i < headers.size(); ++i) {
        std::string h = trim(headers[i]);
        if (iequals(h, "ClOrdID") || iequals(h, "clorderid") || iequals(h, "cl_ord_id")) clIdx = (int)i;
        else if (iequals(h, "order_id_by_algo") || iequals(h, "OrderIDByAlgo")) pIdx = (int)i;
    }
    if (clIdx < 0 || pIdx < 0) {
        LOG(ERROR) << "Map CSV must contain ClOrdID and order_id_by_algo columns.";
        return false;
    }

    while (std::getline(in, line)) {
        if (trim(line).empty()) continue;
        std::vector<std::string> row = parseCsvLine(line);
        if ((int)row.size() <= std::max(clIdx, pIdx)) continue;
        std::string cl = trim(row[(size_t)clIdx]);
        std::string orderIdByAlgo = trim(row[(size_t)pIdx]);
        if (cl.empty() || orderIdByAlgo.empty()) continue;
        clOrdToOrderIdByAlgo[cl] = orderIdByAlgo;
    }
    LOG(INFO) << "Loaded order_id_by_algo map rows: " << clOrdToOrderIdByAlgo.size();
    return true;
}

static void collectCandidatesByParent(const std::string& parentId,
                                      const std::unordered_map<std::string, std::vector<int> >& byParent,
                                      std::unordered_set<int>& out) {
    if (parentId.empty()) return;
    std::unordered_map<std::string, std::vector<int> >::const_iterator it = byParent.find(parentId);
    if (it != byParent.end()) {
        for (size_t i = 0; i < it->second.size(); ++i) out.insert(it->second[i]);
    }
}

static void applyParentScopedCandidates(const std::unordered_set<int>& candidateIds,
                                        std::vector<TargetState>& targets,
                                        const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::vector<ExtractorSpec>& specs,
                                        std::vector<ErrorRow>& errors,
                                        const std::string& sourceFile,
                                        const std::string& sourceLine,
                                        MatchScope scope) {
    for (std::unordered_set<int>::const_iterator it = candidateIds.begin(); it != candidateIds.end(); ++it) {
        int idx = *it;
        if (idx < 0 || (size_t)idx >= targets.size()) continue;
        TargetState& target = targets[(size_t)idx];
        if (scope == MS_PARENT_TEXT) {
            target.parentDebug.sawParentTextLine = true;
            target.parentDebug.parentTextFile = sourceFile;
            target.parentDebug.parentTextExcerpt = lineExcerpt(sourceLine);
        } else if (scope == MS_PARENT_FIX) {
            target.parentDebug.sawParentFixLine = true;
            target.parentDebug.parentFixFile = sourceFile;
            target.parentDebug.parentFixExcerpt = lineExcerpt(sourceLine);
            std::string reason = diagnoseAlgoSenderCompid(fixTags, sourceLine);
            if (!reason.empty()) target.parentDebug.algoSenderMissingReason = reason;
        }
        applyExtractorSpecs(specs, textKv, fixTags, target, errors, sourceFile, sourceLine, scope);
    }
}

static void initializeDefaultOutputs(std::vector<TargetState>& targets,
                                     const std::vector<ExtractorSpec>& specs) {
    for (size_t i = 0; i < targets.size(); ++i) {
        for (size_t s = 0; s < specs.size(); ++s) {
            const ExtractorSpec& spec = specs[s];
            if (spec.source_type == DUAL_TEXT_TAG) {
                targets[i].outValues[spec.out_final_col] = unknownValue();
                targets[i].outValues[spec.out_mismatch_col] = unknownValue();
            } else {
                targets[i].outValues[spec.out_final_col] = unknownValue();
            }
        }
    }
}

static size_t countUnmatchedInGroup(const std::vector<int>& idxs,
                                    const std::vector<TargetState>& targets) {
    size_t n = 0;
    for (size_t i = 0; i < idxs.size(); ++i) {
        int idx = idxs[i];
        if (idx >= 0 && (size_t)idx < targets.size() && !targets[(size_t)idx].matched) ++n;
    }
    return n;
}

static void buildLocalIndexes(const std::vector<int>& idxs,
                              const std::vector<TargetState>& targets,
                              std::unordered_map<std::string, std::vector<int> >& byClOrd,
                              std::unordered_map<std::string, std::vector<int> >& byExec,
                              std::unordered_map<std::string, std::vector<int> >& byParent) {
    byClOrd.clear();
    byExec.clear();
    byParent.clear();

    for (size_t i = 0; i < idxs.size(); ++i) {
        int idx = idxs[i];
        if (idx < 0 || (size_t)idx >= targets.size()) continue;
        const TargetState& t = targets[(size_t)idx];
        if (t.matched) continue;

        byClOrd[t.row.clOrdId].push_back(idx);
        byExec[t.row.execId].push_back(idx);
        if (!t.row.parentId.empty()) byParent[t.row.parentId].push_back(idx);
    }
}

static size_t scanSingleFileForTargets(const std::string& filePath,
                                       const std::vector<int>& targetIdxs,
                                       std::vector<TargetState>& targets,
                                       const std::vector<ExtractorSpec>& specs,
                                       std::vector<ErrorRow>& errors) {
    std::ifstream in(filePath.c_str());
    if (!in.is_open()) return 0;

    std::unordered_map<std::string, std::vector<int> > localByClOrd;
    std::unordered_map<std::string, std::vector<int> > localByExec;
    std::unordered_map<std::string, std::vector<int> > localByParent;
    buildLocalIndexes(targetIdxs, targets, localByClOrd, localByExec, localByParent);

    size_t remaining = countUnmatchedInGroup(targetIdxs, targets);
    size_t matchedNow = 0;

    std::string line;
    while (remaining > 0 && std::getline(in, line)) {
        if (!linePrefilter(line)) continue;

        std::unordered_map<std::string, std::string> textKv = collectTextKVs(line, specs);
        std::string textClOrd = textKv["strtID"];
        std::string textXcid  = textKv["xcid"];

        std::unordered_set<int> textCandidates;
        collectCandidatesByClOrd(textClOrd, localByClOrd, textCandidates);
        collectCandidatesByExec(textXcid, localByExec, textCandidates);

        std::unordered_map<std::string, std::string> emptyFix;
        if (!textCandidates.empty()) {
            size_t newly = applyToCandidates(textCandidates, targets,
                                             textClOrd, textXcid, true,
                                             textKv, emptyFix,
                                             specs, errors,
                                             filePath, line);
            if (newly > 0) {
                matchedNow += newly;
                remaining = (newly >= remaining) ? 0 : (remaining - newly);
            }
        }

        std::string textId = textKv["id"];
        if (!textId.empty() && line.find("strt:0") != std::string::npos) {
            std::unordered_set<int> parentTextCandidates;
            collectCandidatesByParent(textId, localByParent, parentTextCandidates);
            if (!parentTextCandidates.empty()) {
                applyParentScopedCandidates(parentTextCandidates, targets, textKv, emptyFix,
                                            specs, errors, filePath, line, MS_PARENT_TEXT);
            }
        }

        if (line.find("8=FIX") != std::string::npos || line.find("11=") != std::string::npos) {
            std::vector<std::string> msgs = splitFixMessages(line);
            if (msgs.empty()) msgs.push_back(line);

            for (size_t mi = 0; mi < msgs.size() && remaining > 0; ++mi) {
                const std::string& msg = msgs[mi];
                std::unordered_map<std::string, std::string> fixTags = collectFixTags(msg, specs);

                std::string fixClOrd = fixTags["11"];
                std::string fixExec  = fixTags["9607"];
                if (fixExec.empty()) fixExec = fixTags["17"];
                if (fixClOrd.empty() && fixExec.empty()) continue;

                std::unordered_set<int> fixCandidates;
                collectCandidatesByClOrd(fixClOrd, localByClOrd, fixCandidates);
                collectCandidatesByExec(fixExec, localByExec, fixCandidates);
                if (!fixCandidates.empty()) {
                    size_t newly = applyToCandidates(fixCandidates, targets,
                                                     fixClOrd, fixExec, false,
                                                     textKv, fixTags,
                                                     specs, errors,
                                                     filePath, line);
                    if (newly > 0) {
                        matchedNow += newly;
                        remaining = (newly >= remaining) ? 0 : (remaining - newly);
                    }
                }

                if (!fixClOrd.empty()) {
                    std::unordered_set<int> parentFixCandidates;
                    collectCandidatesByParent(fixClOrd, localByParent, parentFixCandidates);
                    if (!parentFixCandidates.empty()) {
                        applyParentScopedCandidates(parentFixCandidates, targets, textKv, fixTags,
                                                    specs, errors, filePath, msg, MS_PARENT_FIX);
                    }
                }
            }
        }
    }

    return matchedNow;
}

static void scanFilesByTargetFile(const std::unordered_map<std::string, std::vector<int> >& fileToTargets,
                                  std::vector<TargetState>& targets,
                                  const std::vector<ExtractorSpec>& specs,
                                  std::vector<ErrorRow>& errors,
                                  std::unordered_set<int>& fallbackTargetIdxs) {
    size_t totalMatched = 0;
    for (std::unordered_map<std::string, std::vector<int> >::const_iterator it = fileToTargets.begin();
         it != fileToTargets.end(); ++it) {
        const std::string& filePath = it->first;
        const std::vector<int>& idxs = it->second;

        if (filePath.empty()) {
            for (size_t i = 0; i < idxs.size(); ++i) fallbackTargetIdxs.insert(idxs[i]);
            continue;
        }

        std::ifstream test(filePath.c_str());
        if (!test.is_open()) {
            for (size_t i = 0; i < idxs.size(); ++i) fallbackTargetIdxs.insert(idxs[i]);
            continue;
        }
        test.close();

        totalMatched += scanSingleFileForTargets(filePath, idxs, targets, specs, errors);
    }
    LOG(INFO) << "Matched in per-file scan: " << totalMatched;
}

static void scanLogDirectorySubset(const std::string& logDir,
                                   const std::vector<int>& targetIdxs,
                                   std::vector<TargetState>& targets,
                                   const std::vector<ExtractorSpec>& specs,
                                   std::vector<ErrorRow>& errors) {
    DIR* dir = opendir(logDir.c_str());
    if (!dir) {
        LOG(ERROR) << "Cannot open directory: " << logDir;
        return;
    }

    std::unordered_map<std::string, std::vector<int> > localByClOrd;
    std::unordered_map<std::string, std::vector<int> > localByExec;
    std::unordered_map<std::string, std::vector<int> > localByParent;
    buildLocalIndexes(targetIdxs, targets, localByClOrd, localByExec, localByParent);

    size_t remaining = countUnmatchedInGroup(targetIdxs, targets);
    size_t matchedNow = 0;

    struct dirent* entry;
    while (remaining > 0 && (entry = readdir(dir)) != NULL) {
        std::string filename = entry->d_name;
        if (filename.empty() || filename[0] == '.' || endsWith(filename, ".gz")) continue;

        std::string fullPath = logDir + "/" + filename;
        std::ifstream in(fullPath.c_str());
        if (!in.is_open()) continue;

        std::string line;
        while (remaining > 0 && std::getline(in, line)) {
            if (!linePrefilter(line)) continue;

            std::unordered_map<std::string, std::string> textKv = collectTextKVs(line, specs);
            std::string textClOrd = textKv["strtID"];
            std::string textXcid  = textKv["xcid"];

            std::unordered_set<int> textCandidates;
            collectCandidatesByClOrd(textClOrd, localByClOrd, textCandidates);
            collectCandidatesByExec(textXcid, localByExec, textCandidates);

            std::unordered_map<std::string, std::string> emptyFix;
            if (!textCandidates.empty()) {
                size_t newly = applyToCandidates(textCandidates, targets,
                                                 textClOrd, textXcid, true,
                                                 textKv, emptyFix,
                                                 specs, errors,
                                                 fullPath, line);
                if (newly > 0) {
                    matchedNow += newly;
                    remaining = (newly >= remaining) ? 0 : (remaining - newly);
                }
            }

            std::string textId = textKv["id"];
            if (!textId.empty() && line.find("strt:0") != std::string::npos) {
                std::unordered_set<int> parentTextCandidates;
                collectCandidatesByParent(textId, localByParent, parentTextCandidates);
                if (!parentTextCandidates.empty()) {
                    applyParentScopedCandidates(parentTextCandidates, targets, textKv, emptyFix,
                                                specs, errors, fullPath, line, MS_PARENT_TEXT);
                }
            }

            if (line.find("8=FIX") != std::string::npos || line.find("11=") != std::string::npos) {
                std::vector<std::string> msgs = splitFixMessages(line);
                if (msgs.empty()) msgs.push_back(line);

                for (size_t mi = 0; mi < msgs.size() && remaining > 0; ++mi) {
                    const std::string& msg = msgs[mi];
                    std::unordered_map<std::string, std::string> fixTags = collectFixTags(msg, specs);

                    std::string fixClOrd = fixTags["11"];
                    std::string fixExec  = fixTags["9607"];
                    if (fixExec.empty()) fixExec = fixTags["17"];
                    if (fixClOrd.empty() && fixExec.empty()) continue;

                    std::unordered_set<int> fixCandidates;
                    collectCandidatesByClOrd(fixClOrd, localByClOrd, fixCandidates);
                    collectCandidatesByExec(fixExec, localByExec, fixCandidates);
                    if (!fixCandidates.empty()) {
                        size_t newly = applyToCandidates(fixCandidates, targets,
                                                         fixClOrd, fixExec, false,
                                                         textKv, fixTags,
                                                         specs, errors,
                                                         fullPath, line);
                        if (newly > 0) {
                            matchedNow += newly;
                            remaining = (newly >= remaining) ? 0 : (remaining - newly);
                        }
                    }

                    if (!fixClOrd.empty()) {
                        std::unordered_set<int> parentFixCandidates;
                        collectCandidatesByParent(fixClOrd, localByParent, parentFixCandidates);
                        if (!parentFixCandidates.empty()) {
                            applyParentScopedCandidates(parentFixCandidates, targets, textKv, fixTags,
                                                        specs, errors, fullPath, msg, MS_PARENT_FIX);
                        }
                    }
                }
            }
        }
    }

    closedir(dir);
    LOG(INFO) << "Matched in fallback directory scan: " << matchedNow;
}

static std::vector<std::string> buildEnrichedColumns(const std::vector<ExtractorSpec>& specs) {
    std::vector<std::string> cols;
    for (size_t i = 0; i < specs.size(); ++i) cols.push_back(specs[i].out_final_col);
    cols.push_back("sor_id");
    cols.push_back("order_id_by_algo");
    return cols;
}

static std::vector<std::string> buildFinalOutputColumns(const std::vector<std::string>& keepOriginalNames,
                                                        const std::vector<std::string>& enrichedCols) {
    std::vector<std::string> base;
    base.reserve(keepOriginalNames.size() + enrichedCols.size() + kExtraEmptyColumns.size());
    for (size_t i = 0; i < keepOriginalNames.size(); ++i) base.push_back(keepOriginalNames[i]);
    for (size_t i = 0; i < enrichedCols.size(); ++i) base.push_back(enrichedCols[i]);
    for (size_t i = 0; i < kExtraEmptyColumns.size(); ++i) base.push_back(kExtraEmptyColumns[i]);

    if (kOutputColumnOrder.empty()) return base;

    std::unordered_set<std::string> seen;
    std::vector<std::string> out;
    out.reserve(kOutputColumnOrder.size() + base.size());

    for (size_t i = 0; i < kOutputColumnOrder.size(); ++i) {
        const std::string& col = kOutputColumnOrder[i];
        if (!col.empty() && seen.insert(col).second) out.push_back(col);
    }
    for (size_t i = 0; i < base.size(); ++i) {
        const std::string& col = base[i];
        if (!col.empty() && seen.insert(col).second) out.push_back(col);
    }
    return out;
}

static bool writeEnrichedCsv(const std::string& outCsv,
                             const std::vector<std::string>& originalHeaders,
                             const std::vector<TargetState>& targets,
                             const std::vector<std::string>& enrichedCols) {
    std::vector<int> keepIdx;
    std::vector<std::string> keepNames;
    for (size_t i = 0; i < originalHeaders.size(); ++i) {
        const std::string& h = originalHeaders[i];
        if (kDropOriginalColumns.count(h)) continue;
        std::string outName = h;
        std::unordered_map<std::string, std::string>::const_iterator itR = kRenameOriginalColumns.find(h);
        if (itR != kRenameOriginalColumns.end()) outName = itR->second;
        keepIdx.push_back((int)i);
        keepNames.push_back(outName);
    }

    std::vector<std::string> finalCols = buildFinalOutputColumns(keepNames, enrichedCols);
    std::ofstream out(outCsv.c_str());
    if (!out.is_open()) {
        LOG(ERROR) << "Failed to open output CSV: " << outCsv;
        return false;
    }

    for (size_t i = 0; i < finalCols.size(); ++i) {
        if (i) out << ",";
        out << csvEscape(finalCols[i]);
    }
    out << "\n";

    for (size_t r = 0; r < targets.size(); ++r) {
        const TargetState& t = targets[r];
        std::unordered_map<std::string, std::string> rowMap;

        for (size_t k = 0; k < keepIdx.size(); ++k) {
            int idx = keepIdx[k];
            const std::string& name = keepNames[k];
            std::string v = (idx >= 0 && (size_t)idx < t.row.cols.size()) ? t.row.cols[(size_t)idx] : "";
            if (v.empty()) v = defaultOriginalMissing(name);
            v = castOriginalColumnValue(name, v);
            rowMap[name] = v;
        }

        for (size_t i = 0; i < enrichedCols.size(); ++i) {
            const std::string& name = enrichedCols[i];
            std::unordered_map<std::string, std::string>::const_iterator it = t.outValues.find(name);
            std::string v = (it == t.outValues.end()) ? std::string("") : it->second;
            if (v.empty() || v == unknownValue()) {
                std::unordered_map<std::string, ValueType>::const_iterator jt = gEnrichedColTypes.find(name);
                ValueType vt = (jt == gEnrichedColTypes.end()) ? VT_STRING : jt->second;
                v = defaultMissing(vt);
            }
            rowMap[name] = v;
        }

        for (size_t i = 0; i < kExtraEmptyColumns.size(); ++i) {
            const std::string& name = kExtraEmptyColumns[i];
            if (!name.empty() && rowMap.find(name) == rowMap.end()) rowMap[name] = "";
        }

        for (size_t c = 0; c < finalCols.size(); ++c) {
            if (c) out << ",";
            const std::string& name = finalCols[c];
            std::unordered_map<std::string, std::string>::const_iterator it = rowMap.find(name);
            const std::string v = (it == rowMap.end()) ? std::string("") : it->second;
            out << csvEscape(v);
        }
        out << "\n";
    }
    return true;
}

static bool writeErrorCsv(const std::string& errCsv,
                          const std::vector<ErrorRow>& errors) {
    std::ofstream out(errCsv.c_str());
    if (!out.is_open()) {
        LOG(ERROR) << "Failed to open error CSV: " << errCsv;
        return false;
    }

    out << "ClOrdID,ExecID,Field,TextValue,TagValue,File,LineExcerpt\n";
    for (size_t i = 0; i < errors.size(); ++i) {
        const ErrorRow& e = errors[i];
        out << csvEscape(e.clOrdId) << ","
            << csvEscape(e.execId) << ","
            << csvEscape(e.field) << ","
            << csvEscape(e.textValue) << ","
            << csvEscape(e.tagValue) << ","
            << csvEscape(e.file) << ","
            << csvEscape(e.excerpt) << "\n";
    }
    return true;
}

static bool writeParentMissingCsv(const std::string& path,
                                  const std::vector<TargetState>& targets) {
    std::ofstream out(path.c_str());
    if (!out.is_open()) {
        LOG(ERROR) << "Failed to open parent missing debug CSV: " << path;
        return false;
    }

    out << "clordid,execid,order_id_by_algo,file_path,saw_parent_text_line,saw_parent_fix_line,missing_algo_id,missing_onleave_internal,missing_onleave_market,missing_algo_sender_compid,algo_sender_missing_reason,parent_text_file,parent_fix_file,parent_text_excerpt,parent_fix_excerpt\n";

    size_t rows = 0;
    for (size_t i = 0; i < targets.size(); ++i) {
        const TargetState& t = targets[i];
        if (t.row.parentId.empty()) continue;

        std::string algoId = "";
        std::string oitn = "";
        std::string omkt = "";
        std::string algoSender = "";

        std::unordered_map<std::string, std::string>::const_iterator it;
        it = t.outValues.find("algo_id"); if (it != t.outValues.end()) algoId = trim(it->second);
        it = t.outValues.find("onleave_internal"); if (it != t.outValues.end()) oitn = trim(it->second);
        it = t.outValues.find("onleave_market"); if (it != t.outValues.end()) omkt = trim(it->second);
        it = t.outValues.find("algo_sender_compid"); if (it != t.outValues.end()) algoSender = trim(it->second);

        bool missingAlgoId = algoId.empty() || algoId == unknownValue();
        bool missingOitn = oitn.empty() || oitn == unknownValue();
        bool missingOmkt = omkt.empty() || omkt == unknownValue();
        bool missingAlgoSender = algoSender.empty() || algoSender == unknownValue();

        if (!missingAlgoId && !missingOitn && !missingOmkt && !missingAlgoSender) continue;
        ++rows;

        std::string reason = t.parentDebug.algoSenderMissingReason;
        if (reason.empty() && missingAlgoSender) {
            if (!t.parentDebug.sawParentFixLine) reason = "parent_fix_line_not_found_in_scanned_scope";
            else reason = "algo_sender_compid_not_populated";
        }

        out << csvEscape(t.row.clOrdId) << ","
            << csvEscape(t.row.execId) << ","
            << csvEscape(t.row.parentId) << ","
            << csvEscape(t.row.filePath) << ","
            << (t.parentDebug.sawParentTextLine ? "1" : "0") << ","
            << (t.parentDebug.sawParentFixLine ? "1" : "0") << ","
            << (missingAlgoId ? "1" : "0") << ","
            << (missingOitn ? "1" : "0") << ","
            << (missingOmkt ? "1" : "0") << ","
            << (missingAlgoSender ? "1" : "0") << ","
            << csvEscape(reason) << ","
            << csvEscape(t.parentDebug.parentTextFile) << ","
            << csvEscape(t.parentDebug.parentFixFile) << ","
            << csvEscape(t.parentDebug.parentTextExcerpt) << ","
            << csvEscape(t.parentDebug.parentFixExcerpt) << "\n";
    }

    LOG(INFO) << "Wrote parent missing debug CSV: " << path << " rows=" << rows;
    return true;
}

static bool runEnrichDiff(const std::string& diffCsv,
                          const std::string& logDir,
                          const std::string& outCsv,
                          const std::string& parentMapCsv) {
    std::vector<ExtractorSpec> specs = buildExtractorSpecs();

    gEnrichedColTypes.clear();
    for (size_t i = 0; i < specs.size(); ++i) gEnrichedColTypes[specs[i].out_final_col] = specs[i].value_type;
    gEnrichedColTypes["sor_id"] = VT_STRING;
    gEnrichedColTypes["order_id_by_algo"] = VT_STRING;

    std::vector<std::string> headers;
    std::vector<TargetState> targets;
    std::unordered_map<std::string, std::vector<int> > byClOrd;
    std::unordered_map<std::string, std::vector<int> > byExec;
    bool hasFileColumn = false;

    if (!loadDiffCsv(diffCsv, headers, targets, byClOrd, byExec, hasFileColumn)) return false;

    std::unordered_map<std::string, std::string> clOrdToParent;
    if (!parentMapCsv.empty()) {
        if (!loadParentMap(parentMapCsv, clOrdToParent)) return false;
    }

    initializeDefaultOutputs(targets, specs);

    for (size_t i = 0; i < targets.size(); ++i) {
        const std::string& cl = targets[i].row.clOrdId;
        targets[i].outValues["sor_id"] = (!cl.empty()) ? getServerNum(cl[0]) : "unknown";
        std::unordered_map<std::string, std::string>::const_iterator itp = clOrdToParent.find(cl);
        if (itp != clOrdToParent.end()) {
            targets[i].row.parentId = itp->second;
            targets[i].outValues["order_id_by_algo"] = itp->second;
        }
    }

    std::vector<ErrorRow> errors;

    if (hasFileColumn) {
        std::unordered_map<std::string, std::vector<int> > fileToTargets;
        std::unordered_set<int> fallbackTargetIdxs;

        for (size_t i = 0; i < targets.size(); ++i) {
            std::string fp = trim(targets[i].row.filePath);
            if (fp.empty()) fallbackTargetIdxs.insert((int)i);
            else fileToTargets[fp].push_back((int)i);
        }

        scanFilesByTargetFile(fileToTargets, targets, specs, errors, fallbackTargetIdxs);

        if (!fallbackTargetIdxs.empty()) {
            if (!logDir.empty()) {
                std::vector<int> fallbackVec;
                fallbackVec.reserve(fallbackTargetIdxs.size());
                for (std::unordered_set<int>::const_iterator it = fallbackTargetIdxs.begin(); it != fallbackTargetIdxs.end(); ++it) {
                    fallbackVec.push_back(*it);
                }
                scanLogDirectorySubset(logDir, fallbackVec, targets, specs, errors);
            }
        }
    } else {
        if (logDir.empty()) {
            LOG(ERROR) << "Diff CSV has no File column and --log_dir is missing.";
            return false;
        }
        std::vector<int> allIdx;
        allIdx.reserve(targets.size());
        for (size_t i = 0; i < targets.size(); ++i) allIdx.push_back((int)i);
        scanLogDirectorySubset(logDir, allIdx, targets, specs, errors);
    }

    std::vector<std::string> enrichedCols = buildEnrichedColumns(specs);
    if (!writeEnrichedCsv(outCsv, headers, targets, enrichedCols)) return false;

    std::string errorPath = "search_results/enrich_errors.csv";
    if (!writeErrorCsv(errorPath, errors)) return false;

    std::string parentMissingPath = "search_results/parent_missing_debug.csv";
    if (!writeParentMissingCsv(parentMissingPath, targets)) return false;

    size_t matchedCount = 0;
    for (size_t i = 0; i < targets.size(); ++i) if (targets[i].matched) ++matchedCount;

    LOG(INFO) << "Matched targets: " << matchedCount << " / " << targets.size();
    LOG(INFO) << "Wrote enriched CSV: " << outCsv;
    LOG(INFO) << "Wrote mismatch errors CSV: " << errorPath << " rows=" << errors.size();
    return true;
}

int main(int argc, char* argv[]) {
    ensureDir("./logs");
    ensureDir("./search_results");
    FLAGS_log_dir = "./logs";
    FLAGS_alsologtostderr = true;
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (argc < 2) {
        std::cerr << "Usage:\n"
                  << "  " << argv[0] << " scan_all <log_directory> [--out <csv_path>]\n"
                  << "  " << argv[0] << " enrich_diff --diff_csv <path> --out_csv <path> [--log_dir <dir>] [--parent_map <csv>]\n";
        return 1;
    }

    std::string mode = argv[1];
    if (mode == "scan_all") {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " scan_all <log_directory> [--out <csv_path>]\n";
            return 1;
        }
        std::string logDir = argv[2];
        std::string outCsv = "search_results/master_executions.csv";
        if (argc >= 5 && std::string(argv[3]) == "--out") outCsv = argv[4];
        processAllLogs(logDir, outCsv);
        return 0;
    }

    if (mode == "enrich_diff") {
        std::string diffCsv;
        std::string logDir;
        std::string outCsv;
        std::string parentMapCsv;

        for (int i = 2; i < argc; ++i) {
            std::string arg = argv[i];
            if ((arg == "--diff_csv" || arg == "-d") && i + 1 < argc) diffCsv = argv[++i];
            else if ((arg == "--log_dir" || arg == "-l") && i + 1 < argc) logDir = argv[++i];
            else if ((arg == "--out_csv" || arg == "-o") && i + 1 < argc) outCsv = argv[++i];
            else if (arg == "--parent_map" && i + 1 < argc) parentMapCsv = argv[++i];
        }

        if (diffCsv.empty() || outCsv.empty()) {
            LOG(ERROR) << "Usage: " << argv[0]
                       << " enrich_diff --diff_csv <path> --out_csv <path> [--log_dir <dir>] [--parent_map <csv>]";
            return 1;
        }

        bool ok = runEnrichDiff(diffCsv, logDir, outCsv, parentMapCsv);
        return ok ? 0 : 1;
    }

    std::cerr << "Unknown mode: " << mode << "\n";
    return 1;
}
