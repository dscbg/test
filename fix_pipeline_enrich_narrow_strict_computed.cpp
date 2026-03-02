// fix_pipeline_updated.cpp
// Unified tool: mode `scan_all` (first pass) + mode `enrich_diff` (second pass).
// C++11-compatible (g++ -std=gnu++11).
//
// Key behaviors:
// 1) STRICT MATCH on (ClOrdID, ExecID) pair: we only mark a target as matched when BOTH values
//    are observed together in the SAME parsing unit (either one FIX msg, or one text line that
//    contains both strtID and xcid).
// 2) No hex/dec conversion for ExecID vs xcid. (xcid is NOT internal id.)
// 3) enrich_diff can optionally use a `File` column in the diff CSV to scan only the specified
//    log file(s). If File is missing/empty/unreadable, it falls back to scanning --log_dir.
// 4) Adding new columns is done by editing buildExtractorSpecs() only.

#ifndef USE_GOOGLE_LOG
#define USE_GOOGLE_LOG
#endif

#include <algorithm>
#include <cctype>
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

static void ensureDir(const std::string& dir) {
    mkdir(dir.c_str(), 0777);
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

static std::string extractTextValueFlexible(const std::string& line, const std::string& key) {
    // STRICT: accept only "key:" style.
    return extractTextValueColon(line, key);
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

static bool linePrefilter(const std::string& line) {
    if (line.find("8=FIX") != std::string::npos) return true;
    if (line.find("strtID:") != std::string::npos) return true;
    if (line.find("xcid:") != std::string::npos) return true;
    if (line.find("11=") != std::string::npos) return true;
    if (line.find("9607=") != std::string::npos) return true;
    if (line.find("17=") != std::string::npos) return true;
    return false;
}

// =========================
// Mode 1: scan_all
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
                    if (raw49.find("ABOVE") == 0 || raw49.find("ABDGW") == 0) {
                        continue;
                    }

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

typedef std::string (*NormalizeFn)(const std::string&);

typedef std::string (*ComputeFn)(const std::unordered_map<std::string, std::string>& textKv,
                                 const std::unordered_map<std::string, std::string>& fixTags,
                                 const std::string& sourceLine);

static std::string normalizeTrim(const std::string& v) { return trim(v); }

struct ExtractorSpec {
    std::string field_name;
    SourceType source_type;
    std::string text_key;      // for TEXT_KV / DUAL
    std::string fix_tag;       // for FIX_TAG / DUAL

    // output columns (for DUAL, we output 4 columns)
    std::string out_text_col;
    std::string out_tag_col;
    std::string out_final_col;
    std::string out_mismatch_col;

    NormalizeFn normalize;
    ComputeFn compute;
};

// =========================
// Computed fields (examples)
// =========================

// Liquidity:
//  - prefer tag 851 when present
//  - otherwise if IOC-like markers exist (tag59=3 or "tif:IOC"), treat as TAKE
static std::string computeLiquidity(const std::unordered_map<std::string, std::string>& /*textKv*/,
                                    const std::unordered_map<std::string, std::string>& /*fixTags*/,
                                    const std::string& line) {
    std::string t851 = getTag(line, "851");
    if (t851 == "1") return "POST";
    if (t851 == "2") return "TAKE";
    if (line.find("59=3") != std::string::npos) return "TAKE";
    if (line.find("tif:IOC") != std::string::npos) return "TAKE";
    return "UNKNOWN";
}

// Visibility:
//  - if both text mflr: and sz: exist, compare them
//  - else if both FIX tags 111 and 38 exist, compare them
//  - else if tag18=M or tag111=0 -> HIDDEN
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

// EDIT HERE to add/remove columns.
static std::vector<ExtractorSpec> buildExtractorSpecs() {
    std::vector<ExtractorSpec> specs;

    // TEXT-only fields (key:value or key value in text log line)
    specs.push_back({"next_index",  TEXT_KV, "nxt",  "", "", "", "next_index",  "", normalizeTrim, NULL});
    specs.push_back({"action",      TEXT_KV, "act",  "", "", "", "action",      "", normalizeTrim, NULL});
    specs.push_back({"wave_index",  TEXT_KV, "wav",  "", "", "", "wave_index",  "", normalizeTrim, NULL});
    specs.push_back({"prev_index",  TEXT_KV, "prv",  "", "", "", "prev_index",  "", normalizeTrim, NULL});
    specs.push_back({"quote_size",  TEXT_KV, "qsz",  "", "", "", "quote_size",  "", normalizeTrim, NULL});
    specs.push_back({"quote_type",  TEXT_KV, "qtyp", "", "", "", "quote_type",  "", normalizeTrim, NULL});
    specs.push_back({"seq_index",   TEXT_KV, "seq",  "", "", "", "seq_index",   "", normalizeTrim, NULL});
    specs.push_back({"exec_id_xcid",TEXT_KV, "xcid", "", "", "", "exec_id_xcid","", normalizeTrim, NULL});

    // DUAL-source fields (text key + FIX tag)
    specs.push_back({"broker_id", DUAL_TEXT_TAG, "brkr", "76",
                     "broker_id_text", "broker_id_tag76", "broker_id_final", "broker_id_mismatch", normalizeTrim, NULL});

    specs.push_back({"target_size", DUAL_TEXT_TAG, "sz", "38",
                     "target_size_text", "target_size_tag38", "target_size_final", "target_size_mismatch", normalizeTrim, NULL});

    // Computed fields: can look at multiple FIX tags and/or multiple text keys on the same line.
    // Output is a single column (plus no mismatch flag).
    specs.push_back({"liquidity",  COMPUTED, "", "", "", "", "liquidity",  "", NULL, &computeLiquidity});
    specs.push_back({"visibility", COMPUTED, "", "", "", "", "visibility", "", NULL, &computeVisibility});

    return specs;
}

struct DiffRow {
    std::vector<std::string> cols;
    std::string clOrdId;
    std::string execId;
    std::string filePath; // optional File column
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
    std::unordered_map<std::string, std::string> outValues;
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
    }
}

static void reconcileDualField(const ExtractorSpec& spec,
                               TargetState& target,
                               std::vector<ErrorRow>& errors,
                               const std::string& sourceFile,
                               const std::string& sourceLine) {
    std::string textVal = unknownValue();
    std::string tagVal  = unknownValue();

    std::unordered_map<std::string, std::string>::iterator itText = target.outValues.find(spec.out_text_col);
    if (itText != target.outValues.end() && !itText->second.empty()) textVal = itText->second;

    std::unordered_map<std::string, std::string>::iterator itTag = target.outValues.find(spec.out_tag_col);
    if (itTag != target.outValues.end() && !itTag->second.empty()) tagVal = itTag->second;

    bool hasText = (textVal != unknownValue());
    bool hasTag  = (tagVal  != unknownValue());

    if (!hasText && !hasTag) {
        target.outValues[spec.out_final_col] = unknownValue();
        target.outValues[spec.out_mismatch_col] = unknownValue();
        return;
    }

    if (hasText && hasTag) {
        if (textVal == tagVal) {
            target.outValues[spec.out_final_col] = textVal;
            target.outValues[spec.out_mismatch_col] = "0";
        } else {
            target.outValues[spec.out_final_col] = "MISMATCH";
            target.outValues[spec.out_mismatch_col] = "1";

            if (!target.mismatchRecorded.count(spec.field_name)) {
                ErrorRow er;
                er.clOrdId = target.row.clOrdId;
                er.execId = target.row.execId;
                er.field = spec.field_name;
                er.textValue = textVal;
                er.tagValue = tagVal;
                er.file = sourceFile;
                er.excerpt = lineExcerpt(sourceLine);
                errors.push_back(er);

                LOG(ERROR) << "Mismatch for " << spec.field_name
                           << " ClOrdID=" << er.clOrdId
                           << " ExecID=" << er.execId
                           << " text='" << er.textValue << "'"
                           << " tag='" << er.tagValue << "'"
                           << " file=" << er.file;
                target.mismatchRecorded.insert(spec.field_name);
            }
        }
        return;
    }

    if (hasText) {
        target.outValues[spec.out_final_col] = textVal;
        target.outValues[spec.out_mismatch_col] = "0";
    } else {
        target.outValues[spec.out_final_col] = tagVal;
        target.outValues[spec.out_mismatch_col] = "0";
    }
}

static void applyExtractorSpecs(const std::vector<ExtractorSpec>& specs,
                                const std::unordered_map<std::string, std::string>& textKv,
                                const std::unordered_map<std::string, std::string>& fixTags,
                                TargetState& target,
                                std::vector<ErrorRow>& errors,
                                const std::string& sourceFile,
                                const std::string& sourceLine) {
    for (size_t i = 0; i < specs.size(); ++i) {
        const ExtractorSpec& spec = specs[i];

        if (spec.source_type == TEXT_KV) {
            std::unordered_map<std::string, std::string>::const_iterator it = textKv.find(spec.text_key);
            if (it != textKv.end()) {
                std::string v = spec.normalize ? spec.normalize(it->second) : it->second;
                if (!v.empty()) setIfUnknown(target.outValues, spec.out_final_col, v);
            }
        } else if (spec.source_type == FIX_TAG) {
            std::unordered_map<std::string, std::string>::const_iterator it = fixTags.find(spec.fix_tag);
            if (it != fixTags.end()) {
                std::string v = spec.normalize ? spec.normalize(it->second) : it->second;
                if (!v.empty()) setIfUnknown(target.outValues, spec.out_final_col, v);
            }
        } else if (spec.source_type == DUAL_TEXT_TAG) {
            // DUAL
            std::unordered_map<std::string, std::string>::const_iterator itText = textKv.find(spec.text_key);
            if (itText != textKv.end()) {
                std::string vText = spec.normalize ? spec.normalize(itText->second) : itText->second;
                if (!vText.empty()) setIfUnknown(target.outValues, spec.out_text_col, vText);
            }

            std::unordered_map<std::string, std::string>::const_iterator itTag = fixTags.find(spec.fix_tag);
            if (itTag != fixTags.end()) {
                std::string vTag = spec.normalize ? spec.normalize(itTag->second) : itTag->second;
                if (!vTag.empty()) setIfUnknown(target.outValues, spec.out_tag_col, vTag);
            }

            reconcileDualField(spec, target, errors, sourceFile, sourceLine);
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
    kv["strtID"] = extractTextValueFlexible(line, "strtID");
    kv["xcid"]  = extractTextValueFlexible(line, "xcid");

    for (size_t i = 0; i < specs.size(); ++i) {
        if (!specs[i].text_key.empty()) {
            kv[specs[i].text_key] = extractTextValueFlexible(line, specs[i].text_key);
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

    for (size_t i = 0; i < specs.size(); ++i) {
        if (!specs[i].fix_tag.empty()) {
            tags[specs[i].fix_tag] = getTag(fixMsg, specs[i].fix_tag);
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

        if (!isTargetConsistent(target, observedClOrd, observedExec, observedExecIsXcid)) {
            continue;
        }

        applyExtractorSpecs(specs, textKv, fixTags, target, errors, sourceFile, sourceLine);

        // STRICT PAIR MATCH: both ids must be present in the SAME parsing unit.
        if (!target.matched && !observedClOrd.empty() && !observedExec.empty()) {
            target.matched = true;
            ++newlyMatched;
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

    while (std::getline(in, line)) {
        if (trim(line).empty()) continue;

        std::vector<std::string> row = parseCsvLine(line);
        if ((int)row.size() <= std::max(clOrdIdx, execIdx)) continue;

        TargetState target;
        target.row.cols = row;
        target.row.clOrdId = trim(row[(size_t)clOrdIdx]);
        target.row.execId  = trim(row[(size_t)execIdx]);
        target.row.filePath = (fileIdx >= 0 && (int)row.size() > fileIdx) ? trim(row[(size_t)fileIdx]) : "";
        target.matched = false;

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

static void initializeDefaultOutputs(std::vector<TargetState>& targets,
                                     const std::vector<ExtractorSpec>& specs) {
    for (size_t i = 0; i < targets.size(); ++i) {
        for (size_t s = 0; s < specs.size(); ++s) {
            const ExtractorSpec& spec = specs[s];
            if (spec.source_type == DUAL_TEXT_TAG) {
                targets[i].outValues[spec.out_text_col] = unknownValue();
                targets[i].outValues[spec.out_tag_col]  = unknownValue();
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
                              std::unordered_map<std::string, std::vector<int> >& byExec) {
    byClOrd.clear();
    byExec.clear();

    for (size_t i = 0; i < idxs.size(); ++i) {
        int idx = idxs[i];
        if (idx < 0 || (size_t)idx >= targets.size()) continue;
        const TargetState& t = targets[(size_t)idx];
        if (t.matched) continue;

        byClOrd[t.row.clOrdId].push_back(idx);
        byExec[t.row.execId].push_back(idx);
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
    buildLocalIndexes(targetIdxs, targets, localByClOrd, localByExec);

    size_t remaining = countUnmatchedInGroup(targetIdxs, targets);
    size_t matchedNow = 0;

    std::string line;
    while (remaining > 0 && std::getline(in, line)) {
        if (!linePrefilter(line)) continue;

        std::unordered_map<std::string, std::string> textKv = collectTextKVs(line, specs);
        std::string textClOrd = textKv["strtID"]; // maps to ClOrdID
        std::string textXcid  = textKv["xcid"];  // maps to ExecID

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

        if (line.find("8=FIX") != std::string::npos || line.find("11=") != std::string::npos) {
            std::vector<std::string> msgs = splitFixMessages(line);
            if (msgs.empty()) msgs.push_back(line);

            for (size_t mi = 0; mi < msgs.size() && remaining > 0; ++mi) {
                const std::string& msg = msgs[mi];
                std::unordered_map<std::string, std::string> fixTags = collectFixTags(msg, specs);

                std::string fixClOrd = fixTags["11"]; // ClOrdID
                std::string fixExec  = fixTags["9607"]; // ExecID
                if (fixExec.empty()) fixExec = fixTags["17"]; // fallback

                if (fixClOrd.empty() && fixExec.empty()) continue;

                std::unordered_set<int> fixCandidates;
                collectCandidatesByClOrd(fixClOrd, localByClOrd, fixCandidates);
                collectCandidatesByExec(fixExec, localByExec, fixCandidates);
                if (fixCandidates.empty()) continue;

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
    buildLocalIndexes(targetIdxs, targets, localByClOrd, localByExec);

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
    // For DUAL_TEXT_TAG fields, we keep output CSV narrow:
    //   - always output only <final> + <mismatch_flag>
    //   - raw text/tag values are written to enrich_errors.csv when mismatch occurs
    // This avoids duplicating columns in the common (matching) case.
    std::vector<std::string> cols;
    for (size_t i = 0; i < specs.size(); ++i) {
        const ExtractorSpec& spec = specs[i];
        if (spec.source_type == DUAL_TEXT_TAG) {
            cols.push_back(spec.out_final_col);
            cols.push_back(spec.out_mismatch_col);
        } else {
            cols.push_back(spec.out_final_col);
        }
    }
    return cols;
}


static bool writeEnrichedCsv(const std::string& outCsv,
                             const std::vector<std::string>& originalHeaders,
                             const std::vector<TargetState>& targets,
                             const std::vector<std::string>& enrichedCols) {
    std::ofstream out(outCsv.c_str());
    if (!out.is_open()) {
        LOG(ERROR) << "Failed to open output CSV: " << outCsv;
        return false;
    }

    for (size_t i = 0; i < originalHeaders.size(); ++i) {
        if (i) out << ",";
        out << csvEscape(originalHeaders[i]);
    }
    for (size_t i = 0; i < enrichedCols.size(); ++i) {
        out << "," << csvEscape(enrichedCols[i]);
    }
    out << "\n";

    for (size_t r = 0; r < targets.size(); ++r) {
        const TargetState& t = targets[r];
        for (size_t c = 0; c < originalHeaders.size(); ++c) {
            if (c) out << ",";
            std::string v = (c < t.row.cols.size()) ? t.row.cols[c] : "";
            out << csvEscape(v.empty() ? unknownValue() : v);
        }

        for (size_t ec = 0; ec < enrichedCols.size(); ++ec) {
            const std::string& col = enrichedCols[ec];
            std::unordered_map<std::string, std::string>::const_iterator it = t.outValues.find(col);
            std::string v = (it == t.outValues.end() || it->second.empty()) ? unknownValue() : it->second;
            out << "," << csvEscape(v);
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
                          const std::string& outCsv) {
    std::vector<ExtractorSpec> specs = buildExtractorSpecs();
    std::vector<std::string> headers;
    std::vector<TargetState> targets;
    std::unordered_map<std::string, std::vector<int> > byClOrd;
    std::unordered_map<std::string, std::vector<int> > byExec;
    bool hasFileColumn = false;

    if (!loadDiffCsv(diffCsv, headers, targets, byClOrd, byExec, hasFileColumn)) {
        return false;
    }

    initializeDefaultOutputs(targets, specs);

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
    if (!writeEnrichedCsv(outCsv, headers, targets, enrichedCols)) {
        return false;
    }

    std::string errorPath = "search_results/enrich_errors.csv";
    if (!writeErrorCsv(errorPath, errors)) {
        return false;
    }

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
                  << "  " << argv[0] << " enrich_diff --diff_csv <path> --out_csv <path> [--log_dir <dir>]\n";
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
        if (argc >= 5 && std::string(argv[3]) == "--out") {
            outCsv = argv[4];
        }
        processAllLogs(logDir, outCsv);
        return 0;
    }

    if (mode == "enrich_diff") {
        std::string diffCsv;
        std::string logDir;
        std::string outCsv;

        for (int i = 2; i < argc; ++i) {
            std::string arg = argv[i];
            if ((arg == "--diff_csv" || arg == "-d") && i + 1 < argc) {
                diffCsv = argv[++i];
            } else if ((arg == "--log_dir" || arg == "-l") && i + 1 < argc) {
                logDir = argv[++i];
            } else if ((arg == "--out_csv" || arg == "-o") && i + 1 < argc) {
                outCsv = argv[++i];
            } else {
                LOG(WARNING) << "Unknown/ignored argument: " << arg;
            }
        }

        if (diffCsv.empty() || outCsv.empty()) {
            LOG(ERROR) << "Usage: " << argv[0]
                       << " enrich_diff --diff_csv <path> --out_csv <path> [--log_dir <dir>]";
            return 1;
        }

        bool ok = runEnrichDiff(diffCsv, logDir, outCsv);
        return ok ? 0 : 1;
    }

    std::cerr << "Unknown mode: " << mode << "\n";
    return 1;
}
