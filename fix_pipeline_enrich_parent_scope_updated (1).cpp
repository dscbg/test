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
        return utcStr; // keep original if parse fails
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


// Map ClOrdID first letter to SOR server number.
static std::string getServerNum(char firstChar) {
    switch (firstChar) {
        case 'C': return "3";
        case 'D': return "8";
        case 'F': return "6";
        case 'E': return "5";
        default:  return "unknown";
    }
}

// Helper: split a line that may contain multiple FIX messages.
static std::vector<std::string> splitFixMessages(const std::string& line) {
    std::vector<std::string> messages;
    const std::string delimiter = "8=FIX";
    size_t pos = line.find(delimiter);

    while (pos != std::string::npos) {
        size_t nextPos = line.find(delimiter, pos + delimiter.length());
        if (nextPos == std::string::npos) {
            messages.push_back(line.substr(pos));
        } else {
            messages.push_back(line.substr(pos, nextPos - pos));
        }
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

// Text form: "key:value"
static std::string extractTextValueColon(const std::string& line, const std::string& key) {
    std::string token = key + ":";
    size_t pos = line.find(token);
    if (pos == std::string::npos) return "";

    size_t start = pos + token.size();
    while (start < line.size() && (line[start] == ' ' || line[start] == '\t')) {
        ++start;
    }

    if (start >= line.size()) return "";

    size_t end = start;
    while (end < line.size()) {
        char c = line[end];
        // We removed ' ' from the break condition so it captures the whole time 
        // string until the bracket '[' starts.
        if (c == '[' || c == '\t' || c == '\r' || c == '\n' || c == ',' || c == '^' || c == '\001') break;
        ++end;
    }
    if (end <= start) return "";
    return trim(line.substr(start, end - start));
}

// static std::string extractTextValueColon(const std::string& line, const std::string& key) {
//     std::string token = key + ":";
//     size_t pos = line.find(token);
//     if (pos == std::string::npos) return "";

//     size_t start = pos + token.size();
//     if (start >= line.size()) return "";

//     size_t end = start;
//     while (end < line.size()) {
//         char c = line[end];
//         if (c == '[' || c == '\t' || c == '\r' || c == '\n' || c == ',' || c == '^' || c == '\001') break;
//         ++end;
//     }
//     if (end <= start) return "";
//     return trim(line.substr(start, end - start));
// }

// Text form: "key value"
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
    // Prefer colon always.
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

// IMPORTANT: no hexToDec for ExecID/xcid; direct string compare only.
static bool execIdMatchesXcid(const std::string& inputExecIdRaw, const std::string& xcidRaw) {
    return iequals(trim(inputExecIdRaw), trim(xcidRaw));
}

// If v is a single char (e.g. "M"), convert to its ASCII integer string ("77").
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

// =========================
// Configuration: original diff CSV columns handling
// =========================
// Drop columns from the original diff CSV in the enriched output (exact header match).

static const std::unordered_set<std::string> kDropOriginalColumns = {
    // e.g. "SomeDebugCol",
    "File"
};

// Rename original columns in the enriched output: oldName -> newName.
static const std::unordered_map<std::string, std::string> kRenameOriginalColumns = {
    {"Exec_ID", "exchange_exec_id"},
    {"LastQty","filled_size_delta"},
    {"Status","status"},
    {"ClOrdID","clorderid"},
    {"Sym","sym"},
    {"Exchange_ID","exch_id"}
};

// Treat these original diff CSV columns as integers when filling missing values.
// Missing -> "0" instead of empty.
static const std::unordered_set<std::string> kOriginalIntColumns = {
    // Common ones (edit as needed):
    "LastQty",
    "CumQty",
    "OrderQty",
};

// Optional: add extra empty columns (they will be created as blank cells in every row).
static const std::vector<std::string> kExtraEmptyColumns = {
    // e.g. "notes", "owner"
};

// Optional: enforce a preferred output order.
// - Put the exact final column names here (after rename), including enriched columns.
// - Columns listed here will appear first (in this order). Any remaining columns are appended after.
// - If you list a column name that doesn't exist, it will be created as an empty column.

static const std::vector<std::string> kOutputColumnOrder = {
    "date", "sym", "time", "sor_id", "side", "next_index", "status", "action",
    "prev_index", "algo_id", "cid", "children", "target_size", "uncommitted_size",
    "filled_size", "last_filled_price", "filled_notional", "committed_size",
    "onleave_internal", "onleave_market", "id", "update_time", "update_time_micros",
    "exchange_transact_time", "creation_time", "index", "price", "exch_id",
    "parent", "original", "algo_sender_compid", "order_id_by_algo", "cross_type",
    "tif", "parent_id", "quote_size", "quote_type", "clorderid", "causal_msg_seqnum",
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

// Optional: cast/transform original diff CSV columns by name before writing (after rename).
// For lightweight needs, keep this empty and handle special casts inline in writeEnrichedCsv.
// Example: if (name == "LastQty") return castToIntString(v);
static std::string castOriginalColumnValue(const std::string& name, const std::string& v) {
    //(void)name;
    if(name == "status") return normalizeCharToAsciiIfSingle(v);
    return v;
}

static inline std::string defaultOriginalMissing(const std::string& headerName) {
    return kOriginalIntColumns.count(headerName) ? "0" : "";
}

// Populated at runtime from buildExtractorSpecs() so writeEnrichedCsv can choose defaults.
static std::unordered_map<std::string, ValueType> gEnrichedColTypes;


static bool linePrefilter(const std::string& line) {
    if (line.find("8=FIX") != std::string::npos) return true;
    if (line.find("strtID:") != std::string::npos || line.find("strtID ") != std::string::npos) return true;
    if (line.find("xcid:") != std::string::npos || line.find("xcid ") != std::string::npos) return true;
    if (line.find("11=") != std::string::npos) return true;
    if (line.find("9607=") != std::string::npos) return true;
    if (line.find("17=") != std::string::npos) return true;
    return false;
}

// =========================
// Mode 1: scan_all (kept as-is; user said ignore, but we keep for completeness)
// =========================

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

// =========================
// Mode 2: enrich_diff
// =========================

enum SourceType { TEXT_KV, FIX_TAG, DUAL_TEXT_TAG, COMPUTED };

// For output defaults ("missing" values) so the enriched CSV can be loaded into kdb cleanly.
// - VT_INT    -> "0"
// - VT_STRING -> "" (empty)
enum ValueType { VT_STRING, VT_INT };
enum MatchScope { MS_CLORD_ONLY, MS_PAIR_STRICT, MS_PARENT_TEXT, MS_PARENT_FIX };

typedef std::string (*NormalizeFn)(const std::string&);
typedef std::string (*ComputeFn)(const std::unordered_map<std::string, std::string>& textKv,
                                 const std::unordered_map<std::string, std::string>& fixTags,
                                 const std::string& sourceLine);

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
    // Identity
    std::string field_name;
    SourceType source_type;

    // Input selectors
    std::string text_key;   // TEXT/DUAL
    std::string fix_tag;    // FIX/DUAL
    bool allow_space;       // whether "key value" is allowed (colon always preferred)

    // Output
    std::string out_final_col;     // always present
    std::string out_mismatch_col;  // only for DUAL (else empty)

    // Normalization / casting
    NormalizeFn normalize_text; // TEXT + DUAL text side
    NormalizeFn normalize_tag;  // FIX  + DUAL tag side

    // Computed
    ComputeFn compute;

    // Output type (used only for choosing default "missing" value).
    ValueType value_type;
    MatchScope match_scope;
};

static inline std::string defaultMissing(ValueType t) {
    return (t == VT_INT) ? "0" : "";
}

// Helper constructors to make specs easy to add.
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

static ExtractorSpec Computed(const std::string& outCol, ComputeFn fn, MatchScope scope = MS_CLORD_ONLY) {
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

// =========================
// Computed fields
// =========================

// Liquidity: prefer tag 851; otherwise infer from IOC markers.
static std::string computeLiquidity(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                    const std::unordered_map<std::string, std::string>& /*fixTags*/,
                                    const std::string& line) {
    std::string t851 = getTag(line, "851");
    if (!t851.empty()) return normalizeCharToAsciiIfSingle(t851);
    std::string t9730 = getTag(line, "9730");
    if(t9730 == "R") return "50";
    if(t9730 == "A") return "49";
    if(!t9730.empty()) return normalizeCharToAsciiIfSingle(t9730);
    // if (t851 == "1") return "POST";
    // if (t851 == "2") return "TAKE";
    if (line.find("59=3") != std::string::npos) return "50";
    if (line.find("tif:IOC") != std::string::npos) return "50";
    return "UNKNOWN";
}

// Visibility: compare mflr vs sz (text) else 111 vs 38 (tags) else infer from 18/111.
static std::string computeVisibility(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                     const std::unordered_map<std::string, std::string>& /*fixTags*/,
                                     const std::string& line) {
    std::string mflr = extractTextValueColon(line, "mflr");
    std::string sz   = extractTextValueColon(line, "sz");
    if (!mflr.empty() && !sz.empty()) {
        return (trim(mflr) == trim(sz)) ? "LIT" : "HIDDEN";
    }

    std::string t111 = getTag(line, "111");
    std::string t38  = getTag(line, "38");
    if (!t111.empty() && !t38.empty()) {
        return (trim(t111) == trim(t38)) ? "LIT" : "HIDDEN";
    }

    std::string t18 = getTag(line, "18");
    if (t18 == "M" || t111 == "0") return "HIDDEN";
    return "UNKNOWN";
}

// SendingTime (tag 60) converted to EST (-5h).
static std::string computeSendingTimeEstFull(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                            const std::unordered_map<std::string, std::string>& /*fixTags*/,
                                            const std::string& line) {
    std::string t60 = getTag(line, "60");
    if (t60.empty()) return "";
    return utcToEstFixed5(trim(t60));
}

// Split sending_time_est into date/time columns.
// Output format from utcToEstFixed5: YYYYMMDD-HH:MM:SS[.sss]
static std::string computeSendingDateEst(const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    (void)textKv; (void)fixTags;
    std::string est = computeSendingTimeEstFull(textKv, fixTags, line);
    if (est.size() < 8) return "UNKNOWN";
    return est.substr(0, 8);
}

static std::string computeSendingTimeEst(const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    
    std::string est = extractTextValueColon(line,"INFO");
    if (est.empty()) return "UNKNOWN";
    return est;
}

static std::string computeSendingTimeEstMilli(const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    int hh, mm, ss, s_sub;
    sscanf(est.c_str(), "%d:%d:%d.%d", &hh, &mm, &ss, &s_sub);

    long long totalMillis = (hh * 3600000LL) + (mm * 60000LL) + (ss * 1000LL) + (s_sub / 1000);
    return std::to_string(totalMillis);
    
}
static std::string computeSendingTimeEstTime(const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    if (est.length() >= 12) {
        std::string milliTime = est.substr(0, 12);
        return milliTime;
}
    return est;
    
}

static std::string computeSendingTimeEstMicro(const std::unordered_map<std::string, std::string>& textKv,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    
    std::string est = computeSendingTimeEst(textKv,fixTags,line);
    if(est=="UNKOWN") return "";
    int hh, mm, ss, fractional;
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
    int hh, mm, ss, fractional;
    if (sscanf(est.c_str(), "%d:%d:%d.%d", &hh, &mm, &ss, &fractional) == 4) {
        
        long long baseMillis = (hh * 3600000LL) + (mm * 60000LL) + (ss * 1000LL);
        int millisPart = fractional / 1000;
        int microRemainder = fractional % 1000; 

        // 1. Calculate and Round Milliseconds
        long long roundedMillis = baseMillis + millisPart;
        if (microRemainder >= 500) {
            roundedMillis += 1;
        }

        // 2. Convert to Microseconds (Adding three zeros)
        long long finalMicros = roundedMillis * 1000LL;
        return std::to_string(finalMicros);

    }
    return "";
    
}
static std::string computeAlgoSenderId(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                        const std::unordered_map<std::string, std::string>& fixTags,
                                        const std::string& line) {
    std::string t11;
    std::unordered_map<std::string, std::string>::const_iterator it11 = fixTags.find("11");
    if (it11 != fixTags.end()) t11 = trim(it11->second);
    if (t11.empty()) t11 = getTag(line, "11");

    std::string t49;
    std::unordered_map<std::string, std::string>::const_iterator it49 = fixTags.find("49");
    if (it49 != fixTags.end()) t49 = trim(it49->second);
    if (t49.empty()) t49 = getTag(line, "49");
    if (t49.empty()) {
        LOG(WARNING) << "[algo_sender_compid] missing tag49"
                     << " parent_id=" << t11
                     << " line=" << lineExcerpt(line);
        return "";
    }

    if (t49.compare(0, 4, "CASH") == 0) {
        std::string t50;
        std::unordered_map<std::string, std::string>::const_iterator it50 = fixTags.find("50");
        if (it50 != fixTags.end()) t50 = trim(it50->second);
        if (t50.empty()) t50 = getTag(line, "50");
        if (t50.empty()) {
            LOG(WARNING) << "[algo_sender_compid] CASH sender but missing tag50"
                         << " parent_id=" << t11
                         << " tag49=" << t49
                         << " line=" << lineExcerpt(line);
            return "";
        }
        if (t50.size() < 3) {
            LOG(WARNING) << "[algo_sender_compid] CASH sender tag50 shorter than 3 chars"
                         << " parent_id=" << t11
                         << " tag49=" << t49
                         << " tag50=" << t50
                         << " line=" << lineExcerpt(line);
            return "";
        }
        return t49 + t50.substr(0, 3);
    }

    std::string t9702;
    std::unordered_map<std::string, std::string>::const_iterator it9702 = fixTags.find("9702");
    if (it9702 != fixTags.end()) t9702 = trim(it9702->second);
    if (t9702.empty()) t9702 = getTag(line, "9702");
    if (t9702.empty()) {
        LOG(WARNING) << "[algo_sender_compid] missing tag9702"
                     << " parent_id=" << t11
                     << " tag49=" << t49
                     << " line=" << lineExcerpt(line);
        return "";
    }
    return t49 + t9702;
}

static std::string trimToMillis(const std::string& s) {
    std::string t = trim(s);
    size_t dot = t.find('.');
    if (dot == std::string::npos) return t;
    if (dot + 4 <= t.size()) return t.substr(0, dot + 4);
    return t;
}

static std::string computeCreationTime(const std::unordered_map<std::string, std::string>& /*textKv*/,
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

// static std::string computeSendingTimeEst(const std::unordered_map<std::string, std::string>& textKv,
//                                         const std::unordered_map<std::string, std::string>& fixTags,
//                                         const std::string& line) {
//     (void)textKv; (void)fixTags;
//     std::string est = computeSendingTimeEstFull(textKv, fixTags, line);
//     size_t dash = est.find('-');
//     if (dash == std::string::npos || dash + 1 >= est.size()) return "UNKNOWN";
//     return est.substr(dash + 1);
// }
static std::string computeNotional(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                            const std::unordered_map<std::string, std::string>& /*fixTags*/,
                                            const std::string& line) {
    std::string t6 = getTag(line, "6");
    std::string t14 = getTag(line,"14");
    if(t6.empty() || t14.empty()) return "";
    return std::to_string(std::stod(t6) * std::stod(t14));
}
static std::vector<ExtractorSpec> buildExtractorSpecs() {
    std::vector<ExtractorSpec> specs;

    // TEXT-only fields bound to current ClOrdID only (no ExecID required)
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

    // Parent-derived text fields: use strtID:parent_id line
    specs.push_back(Text("algo_id", "aid", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;
    specs.push_back(Text("onleave_internal", "oitn", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;
    specs.push_back(Text("onleave_market", "omkt", normalizeTrim, false, MS_PARENT_TEXT)); specs.back().value_type = VT_INT;

    // FIX-only fields bound to strict (ClOrdID, ExecID) pair
    specs.push_back(Tag("causal_msg_seqnum", "34")); specs.back().value_type = VT_INT;
    specs.push_back(Tag("tif", "59", normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT;

    // DUAL fields (strict pair only)
    specs.push_back(Dual("side", "sd", "54", normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_STRING;
    specs.push_back(Dual("price", "px", "44")); specs.back().value_type = VT_STRING;
    specs.push_back(Dual("filled_size", "fll", "14")); specs.back().value_type = VT_INT;
    specs.push_back(Dual("exec_inst", "t18", "18", normalizeTrim, normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT;
    specs.push_back(Dual("broker_id", "brkr", "76")); specs.back().value_type = VT_INT;
    specs.push_back(Dual("target_size", "sz", "38")); specs.back().value_type = VT_INT;
    specs.push_back(Dual("cross_type", "aggr", "40", normalizeCharToAsciiIfSingle, normalizeCharToAsciiIfSingle)); specs.back().value_type = VT_INT;

    // COMPUTED fields on current-clOrd lines
    specs.push_back(Computed("filled_notional", &computeNotional)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("liquidity_indicator", &computeLiquidity)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("date", &computeSendingDateEst)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("update_time", &computeSendingTimeEstMilli)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("update_time_micros", &computeSendingTimeEstMicro)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("time", &computeSendingTimeEstTime)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("exchange_transact_time", &computeSendingTimeEstTransact)); specs.back().value_type = VT_STRING;
    specs.push_back(Computed("creation_time", &computeCreationTime)); specs.back().value_type = VT_STRING;

    // Parent-derived FIX computed field: use FIX line where 11=parent_id
    specs.push_back(Computed("algo_sender_compid", &computeAlgoSenderId, MS_PARENT_FIX)); specs.back().value_type = VT_STRING;

    return specs;
}

// =========================
// Data structures
// =========================

struct DiffRow {
    std::vector<std::string> cols;
    std::string clOrdId;
    std::string execId;
    std::string filePath; // optional File column
    std::string parentId; // optional parent from external map
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

struct TargetState {
    DiffRow row;
    std::unordered_map<std::string, std::string> outValues; // final output columns + mismatch flags
    std::unordered_set<std::string> mismatchRecorded;
    bool matched;
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

// DUAL policy:
// - both present and equal -> final=value, mismatch=0
// - both present and not equal -> final=MISMATCH, mismatch=1, plus error row
// - only one present -> final=that value, mismatch=0
// - neither present -> final=UNKNOWN, mismatch=UNKNOWN
static void applyDualField(const ExtractorSpec& spec,
                           const std::string& maybeTextRaw,
                           const std::string& maybeTagRaw,
                           TargetState& target,
                           std::vector<ErrorRow>& /*errors*/,
                           const std::string& /*sourceFile*/,
                           const std::string& /*sourceLine*/) {
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

    if (hasText && hasTag && textVal != tagVal) {
        LOG(WARNING) << "Source conflict for " << spec.out_final_col
                     << " ClOrdID=" << target.row.clOrdId
                     << " ExecID=" << target.row.execId
                     << " text='" << textVal << "' tag='" << tagVal << "'";
    }

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
    kv["strtID"] = extractTextValueFlexible(line, "strtID", /*allowSpace=*/true);
    kv["xcid"]   = extractTextValueFlexible(line, "xcid",   /*allowSpace=*/true);

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
            if (!specs[i].fix_tag.empty()) {
                tags[specs[i].fix_tag] = getTag(fixMsg, specs[i].fix_tag);
            }
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

        if (hasClOrd) {
            applyExtractorSpecs(specs, textKv, fixTags, target, errors, sourceFile, sourceLine, MS_CLORD_ONLY);
        }
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

    // Record invalid ClOrdID rows (length != 14) so you can inspect / fix upstream.
    ensureDir("search_results");
    std::string invalidPath = "search_results/invalid_diff_rows.csv";
    std::ofstream invalidOut(invalidPath.c_str(), std::ios::app);
    bool invalidNew = false;
    {
        std::ifstream chk(invalidPath.c_str());
        invalidNew = (!chk.is_open() || chk.peek() == std::ifstream::traits_type::eof());
    }
    if (invalidNew && invalidOut.is_open()) {
        invalidOut << "LineNo,ClOrdID,ExecID,Reason,RowRaw\n";
    }

    size_t lineNo = 1; // header already read

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
                invalidOut << lineNo << ","
                           << csvEscape(target.row.clOrdId) << ","
                           << csvEscape(target.row.execId) << ","
                           << "ClOrdID_length_not_14," << csvEscape(line) << "\n";
            }
            continue;
        }

        std::string pairKey = target.row.clOrdId + "\x1f" + target.row.execId;
        if (uniquePairs.count(pairKey)) {
            LOG(WARNING) << "Duplicate pair in diff CSV, skipping: " << target.row.clOrdId
                         << " / " << target.row.execId;
            continue;
        }
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
                          std::unordered_map<std::string, std::string>& clOrdToParent) {
    clOrdToParent.clear();
    if (mapCsv.empty()) return true;

    std::ifstream in(mapCsv.c_str());
    if (!in.is_open()) {
        LOG(ERROR) << "Failed to open parent map CSV: " << mapCsv;
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
        else if (iequals(h, "parent") || iequals(h, "parent_id") || iequals(h, "ParentID")) pIdx = (int)i;
    }
    if (clIdx < 0 || pIdx < 0) {
        LOG(ERROR) << "Parent map CSV must contain ClOrdID and parent_id columns.";
        return false;
    }

    while (std::getline(in, line)) {
        if (trim(line).empty()) continue;
        std::vector<std::string> row = parseCsvLine(line);
        if ((int)row.size() <= std::max(clIdx, pIdx)) continue;
        std::string cl = trim(row[(size_t)clIdx]);
        std::string parent = trim(row[(size_t)pIdx]);
        if (cl.empty() || parent.empty()) continue;
        clOrdToParent[cl] = parent;
    }
    LOG(INFO) << "Loaded parent map rows: " << clOrdToParent.size();
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

        if (!textClOrd.empty()) {
            std::unordered_set<int> parentTextCandidates;
            collectCandidatesByParent(textClOrd, localByParent, parentTextCandidates);
            if (!parentTextCandidates.empty()) {
                LOG(INFO) << "[parent_text_match] parent_id=" << textClOrd
                          << " candidates=" << parentTextCandidates.size()
                          << " file=" << filePath;
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
                        LOG(INFO) << "[parent_fix_match] parent_id=" << fixClOrd
                                  << " candidates=" << parentFixCandidates.size()
                                  << " file=" << filePath
                                  << " line=" << lineExcerpt(msg);
                        applyParentScopedCandidates(parentFixCandidates, targets, textKv, fixTags,
                                                    specs, errors, filePath, line, MS_PARENT_FIX);
                    }
                }
            }
        }
    }

    for (size_t i = 0; i < targetIdxs.size(); ++i) {
        int idx = targetIdxs[i];
        if (idx < 0 || (size_t)idx >= targets.size()) continue;
        const TargetState& t = targets[(size_t)idx];
        if (t.row.parentId.empty()) continue;

        std::unordered_map<std::string, std::string>::const_iterator itAlgo =
            t.outValues.find("algo_sender_compid");
        std::string algoSender = (itAlgo == t.outValues.end()) ? "" : trim(itAlgo->second);
        if (algoSender.empty() || algoSender == unknownValue()) {
            LOG(WARNING) << "[algo_sender_compid] unresolved after scanning current target file"
                         << " clordid=" << t.row.clOrdId
                         << " execid=" << t.row.execId
                         << " parent_id=" << t.row.parentId
                         << " file=" << filePath;
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
            LOG(WARNING) << "Target file not accessible, fallback to --log_dir for these targets: " << filePath;
            for (size_t i = 0; i < idxs.size(); ++i) fallbackTargetIdxs.insert(idxs[i]);
            continue;
        }
        test.close();

        size_t matchedNow = scanSingleFileForTargets(filePath, idxs, targets, specs, errors);
        totalMatched += matchedNow;
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

            if (!textClOrd.empty()) {
                std::unordered_set<int> parentTextCandidates;
                collectCandidatesByParent(textClOrd, localByParent, parentTextCandidates);
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
                    if (fixCandidates.empty()) continue;

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
            }
        }
    }

    closedir(dir);
    LOG(INFO) << "Matched in fallback directory scan: " << matchedNow;
}

static std::vector<std::string> buildEnrichedColumns(const std::vector<ExtractorSpec>& specs) {
    // Output CSV is intentionally narrow:
    // - One column per spec (spec.out_final_col) only.
    // - Any mismatches are recorded in search_results/enrich_errors.csv with full details.
    std::vector<std::string> cols;
    for (size_t i = 0; i < specs.size(); ++i) {
        const ExtractorSpec& spec = specs[i];
        cols.push_back(spec.out_final_col);
    }
    
    // Derived / prefilled columns
    cols.push_back("sor_id");
    cols.push_back("parent_id");
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

    // First: explicit order (create empty columns if missing).
    for (size_t i = 0; i < kOutputColumnOrder.size(); ++i) {
        const std::string& col = kOutputColumnOrder[i];
        if (col.empty()) continue;
        if (seen.insert(col).second) out.push_back(col);
    }

    // Then: remaining columns in default order.
    for (size_t i = 0; i < base.size(); ++i) {
        const std::string& col = base[i];
        if (col.empty()) continue;
        if (seen.insert(col).second) out.push_back(col);
    }

    return out;
}

static bool writeEnrichedCsv(const std::string& outCsv,
                             const std::vector<std::string>& originalHeaders,
                             const std::vector<TargetState>& targets,
                             const std::vector<std::string>& enrichedCols) {
    // Decide which original headers to keep, applying drop/rename rules.
    std::vector<int> keepIdx;
    std::vector<std::string> keepNames;
    keepIdx.reserve(originalHeaders.size());
    keepNames.reserve(originalHeaders.size());

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

    // Header
    for (size_t i = 0; i < finalCols.size(); ++i) {
        if (i) out << ",";
        out << csvEscape(finalCols[i]);
    }
    out << "\n";

    // Rows
    for (size_t r = 0; r < targets.size(); ++r) {
        const TargetState& t = targets[r];

        // Build a name->value map for this row (after rename).
        std::unordered_map<std::string, std::string> rowMap;
        rowMap.reserve(keepNames.size() + enrichedCols.size() + kExtraEmptyColumns.size() + 8);

        // Original columns
        for (size_t k = 0; k < keepIdx.size(); ++k) {
            int idx = keepIdx[k];
            const std::string& name = keepNames[k];
            std::string v = (idx >= 0 && (size_t)idx < t.row.cols.size()) ? t.row.cols[(size_t)idx] : "";
            if (v.empty()) v = defaultOriginalMissing(name);
            v = castOriginalColumnValue(name, v);
            rowMap[name] = v;
        }

        // Enriched columns
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

        // Extra empty columns (explicitly blank, not UNKNOWN)
        for (size_t i = 0; i < kExtraEmptyColumns.size(); ++i) {
            const std::string& name = kExtraEmptyColumns[i];
            if (!name.empty() && rowMap.find(name) == rowMap.end()) rowMap[name] = "";
        }

        // Write in final column order
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

static bool runEnrichDiff(const std::string& diffCsv,
                          const std::string& logDir,
                          const std::string& outCsv,
                          const std::string& parentMapCsv) {
    std::vector<ExtractorSpec> specs = buildExtractorSpecs();

    // Build enriched column -> type map for type-aware default missing values.
    gEnrichedColTypes.clear();
    for (size_t i = 0; i < specs.size(); ++i) {
        gEnrichedColTypes[specs[i].out_final_col] = specs[i].value_type;
    }
    gEnrichedColTypes["sor_id"] = VT_STRING;
    gEnrichedColTypes["parent_id"] = VT_STRING;
    gEnrichedColTypes["order_id_by_algo"] = VT_STRING;
    std::vector<std::string> headers;
    std::vector<TargetState> targets;
    std::unordered_map<std::string, std::vector<int> > byClOrd;
    std::unordered_map<std::string, std::vector<int> > byExec;
    bool hasFileColumn = false;

    if (!loadDiffCsv(diffCsv, headers, targets, byClOrd, byExec, hasFileColumn)) {
        return false;
    }

    std::unordered_map<std::string, std::string> clOrdToParent;
    if (!parentMapCsv.empty()) {
        if (!loadParentMap(parentMapCsv, clOrdToParent)) return false;
    }

    initializeDefaultOutputs(targets, specs);

    // Fill row-derived columns that do not require log scanning.
    for (size_t i = 0; i < targets.size(); ++i) {
        const std::string& cl = targets[i].row.clOrdId;
        if (!cl.empty()) targets[i].outValues["sor_id"] = getServerNum(cl[0]);
        else targets[i].outValues["sor_id"] = "unknown";

        std::unordered_map<std::string, std::string>::const_iterator itp = clOrdToParent.find(cl);
        if (itp != clOrdToParent.end()) {
            // parent/order_id_by_algo stays as raw string; do NOT hex-normalize it.
            targets[i].row.parentId = itp->second;
            targets[i].outValues["parent"] = itp->second;
            targets[i].outValues["parent_id"] = itp->second;
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
            if (logDir.empty()) {
                LOG(WARNING) << "Some targets have empty/invalid File path, but --log_dir is not provided."
                             << " Remaining targets may stay UNKNOWN.";
            } else {
                std::vector<int> fallbackVec;
                fallbackVec.reserve(fallbackTargetIdxs.size());
                for (std::unordered_set<int>::const_iterator it = fallbackTargetIdxs.begin();
                     it != fallbackTargetIdxs.end(); ++it) {
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

    size_t matchedCount = 0;
    for (size_t i = 0; i < targets.size(); ++i) if (targets[i].matched) ++matchedCount;

    LOG(INFO) << "Matched targets: " << matchedCount << " / " << targets.size();
    LOG(INFO) << "Wrote enriched CSV: " << outCsv;
    LOG(INFO) << "Wrote mismatch errors CSV: " << errorPath << " rows=" << errors.size();
    return true;
}

// =========================
// main
// =========================
//
// Usage:
//   ./fix_pipeline scan_all <log_directory> [--out search_results/master_executions.csv]
//   ./fix_pipeline enrich_diff --diff_csv <path> --out_csv <path> [--log_dir <dir>]
//
// Note: for enrich_diff, if diff CSV has `File` column, --log_dir becomes optional.

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
            else LOG(WARNING) << "Unknown/ignored argument: " << arg;
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
