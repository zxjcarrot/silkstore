#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "silkstore/silkstore_impl.h"
#include "silkstore/util.h"

#include <string>

namespace leveldb {
namespace silkstore {

static std::string RandomString(Random *rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
}

using std::string;

class MinirunTest {
};

TEST(MinirunTest, BuilderTestSmall
) {
string fname = TempFileName("/tmp", 1239);
WritableFile *file = nullptr;
Status s = Env::Default()->NewWritableFile(fname, &file);
ASSERT_OK(s);
Options options;
MiniRunBuilder builder(options, file, 0);
builder.Add("k1", "v1");
builder.Add("k2", "v2");
builder.Add("k3", "v3");
s = builder.Finish();
ASSERT_OK(s);
ASSERT_EQ(builder
.

NumEntries(),

3);
size_t run_size = builder.FileSize();
Block index_block(BlockContents{builder.IndexBlock(), false, false});

RandomAccessFile *rndfile;
s = Env::Default()->NewRandomAccessFile(fname, &rndfile);
ASSERT_OK(s);
MiniRun run(&options, rndfile, 0, run_size, index_block);
Iterator *run_iter = run.NewIterator(ReadOptions());
DeferCode code([&run_iter]() { delete run_iter; });

run_iter->

SeekToFirst();

ASSERT_EQ(run_iter
->

Valid(),

true);
ASSERT_EQ(run_iter
->

key()

.

ToString(),

"k1");
ASSERT_EQ(run_iter
->

value()

.

ToString(),

"v1");
run_iter->

SeekToLast();

ASSERT_EQ(run_iter
->

Valid(),

true);
ASSERT_EQ(run_iter
->

key()

.

ToString(),

"k3");
ASSERT_EQ(run_iter
->

value()

.

ToString(),

"v3");
run_iter->

Prev();

ASSERT_EQ(run_iter
->

Valid(),

true);
ASSERT_EQ(run_iter
->

key()

.

ToString(),

"k2");
ASSERT_EQ(run_iter
->

value()

.

ToString(),

"v2");
run_iter->

Prev();

ASSERT_EQ(run_iter
->

Valid(),

true);
ASSERT_EQ(run_iter
->

key()

.

ToString(),

"k1");
ASSERT_EQ(run_iter
->

value()

.

ToString(),

"v1");
}

TEST(MinirunTest, BuilderTestLarge
) {
static int kNum = 100000;
string fname = TempFileName("/tmp", 1239);
WritableFile *file = nullptr;
Status s = Env::Default()->NewWritableFile(fname, &file);
ASSERT_OK(s);
Options options;
MiniRunBuilder builder(options, file, 0);
std::vector <string> keys(kNum);
std::vector <string> values(kNum);
Random rnd(0);
for (
int i = 0;
i<kNum;
++i) {
char key_buf[100];
snprintf(key_buf,
sizeof(key_buf), "key%06ld", i);
keys[i] =
string(key_buf,
9);
values[i] =
RandomString(&rnd,
100);
builder.
Add(keys[i], values[i]
);
}
s = builder.Finish();
ASSERT_OK(s);
ASSERT_EQ(builder
.

NumEntries(), kNum

);
size_t run_size = builder.FileSize();
Block index_block(BlockContents{builder.IndexBlock(), false, false});

RandomAccessFile *rndfile;
s = Env::Default()->NewRandomAccessFile(fname, &rndfile);
ASSERT_OK(s);
MiniRun run(&options, rndfile, 0, run_size, index_block);
Iterator *run_iter = run.NewIterator(ReadOptions());
DeferCode code([&run_iter]() { delete run_iter; });

run_iter->

SeekToFirst();

for (
int i = 0;
i<kNum;
++i) {
ASSERT_EQ(run_iter
->

Valid(),

true);
ASSERT_EQ(run_iter
->

key()

.

ToString(), keys[i]

);
ASSERT_EQ(run_iter
->

value()

.

ToString(), values[i]

);
run_iter->

Next();
}
ASSERT_EQ(run_iter
->

Valid(),

false);
file->

Close();
}


TEST(MinirunTest, MiniRunIndexEntryTest
) {
string index_block_contents = "index_block";
string filter_block_contents = "filter_data";

string buf1;
auto e1 = MiniRunIndexEntry::Build(0, 0, Slice(index_block_contents), Slice(filter_block_contents), 0, &buf1);

ASSERT_EQ(e1
.

GetSegmentNumber(),

0);
ASSERT_EQ(e1
.

GetRunNumberWithinSegment(),

0);
ASSERT_EQ(e1
.

GetRunDataSize(),

0);
ASSERT_EQ(e1
.

GetBlockIndexData()

.

ToString(), index_block_contents

);
ASSERT_EQ(e1
.

GetFilterData()

.

ToString(), filter_block_contents

);

string buf2;
auto e2 = MiniRunIndexEntry::Build(2, 1, Slice(index_block_contents), Slice(filter_block_contents), 0, &buf2);
ASSERT_EQ(e2
.

GetSegmentNumber(),

2);
ASSERT_EQ(e2
.

GetRunNumberWithinSegment(),

1);
ASSERT_EQ(e2
.

GetRunDataSize(),

0);
ASSERT_EQ(e2
.

GetBlockIndexData()

.

ToString(), index_block_contents

);
ASSERT_EQ(e2
.

GetFilterData()

.

ToString(), filter_block_contents

);

MiniRunIndexEntry e1_dup(e1.GetRawData());
ASSERT_EQ(e1_dup
.

GetFilterData()

.

ToString(), e1

.

GetFilterData()

.

ToString()

);
ASSERT_EQ(e1_dup
.

GetRunDataSize(), e1

.

GetRunDataSize()

);
ASSERT_EQ(e1_dup
.

GetBlockIndexData()

.

ToString(), e1

.

GetBlockIndexData()

.

ToString()

);
ASSERT_EQ(e1_dup
.

GetRunNumberWithinSegment(), e1

.

GetRunNumberWithinSegment()

);
ASSERT_EQ(e1_dup
.

GetSegmentNumber(), e1

.

GetSegmentNumber()

);
}

TEST(MinirunTest, LeafIndexEntryTest
) {
{
LeafIndexEntry base;
ASSERT_TRUE(base
.

Empty()

);
string buf1;
auto e1 = MiniRunIndexEntry::Build(0, 0, Slice(), Slice(), 0, &buf1);
string buf;
LeafIndexEntry new_entry;
LeafIndexEntryBuilder::AppendMiniRunIndexEntry(base, e1, &buf, &new_entry
);
ASSERT_EQ(new_entry
.

Empty(),

false);
ASSERT_EQ(new_entry
.

GetNumMiniRuns(),

1);
auto minirun_index_entries = new_entry.GetAllMiniRunIndexEntry();
ASSERT_EQ(minirun_index_entries
.

size(),

1);
ASSERT_EQ(minirun_index_entries[0]
.

GetSegmentNumber(),

0);
ASSERT_EQ(minirun_index_entries[0]
.

GetRunDataSize(),

0);
ASSERT_EQ(minirun_index_entries[0]
.

GetRunNumberWithinSegment(),

0);
ASSERT_EQ(minirun_index_entries[0]
.

GetBlockIndexData()

.

ToString(), Slice()

.

ToString()

);
ASSERT_EQ(minirun_index_entries[0]
.

GetFilterData()

.

ToString(), Slice()

.

ToString()

);
}

{
static int eNum = 128;
LeafIndexEntry base_entry, new_entry;
std::vector <string> bufs(eNum);
std::vector <string> new_entry_bufs(eNum);
for (
int i = 0;
i<eNum;
++i) {
auto e1 = MiniRunIndexEntry::Build(i, i, Slice(std::to_string(i)), Slice(std::to_string(i)), 0, &bufs[i]);
LeafIndexEntryBuilder::AppendMiniRunIndexEntry(base_entry, e1, &new_entry_bufs[i], &new_entry
);
base_entry = new_entry;
}

{
ASSERT_EQ(new_entry
.

GetNumMiniRuns(), eNum

);
int i = 0;
new_entry.ForEachMiniRunIndexEntry([&i](
const MiniRunIndexEntry &e, uint32_t
run_no) -> bool{
ASSERT_EQ(run_no, i
);
ASSERT_EQ(e
.

GetSegmentNumber(), i

);
ASSERT_EQ(e
.

GetRunNumberWithinSegment(), i

);
ASSERT_EQ(e
.

GetFilterData()

.

ToString(), std::to_string(i)

);
ASSERT_EQ(e
.

GetBlockIndexData()

.

ToString(), std::to_string(i)

);
++
i;
return false;
}, LeafIndexEntry::TraversalOrder::forward);
i = eNum - 1;
new_entry.ForEachMiniRunIndexEntry([&i](
const MiniRunIndexEntry &e, uint32_t
run_no) -> bool{
ASSERT_EQ(run_no, i
);
ASSERT_EQ(e
.

GetSegmentNumber(), i

);
ASSERT_EQ(e
.

GetRunNumberWithinSegment(), i

);
ASSERT_EQ(e
.

GetFilterData()

.

ToString(), std::to_string(i)

);
ASSERT_EQ(e
.

GetBlockIndexData()

.

ToString(), std::to_string(i)

);
--
i;
return false;
}, LeafIndexEntry::TraversalOrder::backward);
}
ASSERT_EQ(base_entry
.

GetNumMiniRuns(), eNum

);
ASSERT_EQ(new_entry
.

GetNumMiniRuns(), eNum

);
{
for (
int i = 0;
i<eNum;
++i) {
bufs[0].

clear();

new_entry_bufs[0].

clear();

auto e = MiniRunIndexEntry::Build(eNum * 2, eNum * 2, Slice(std::to_string(eNum * 2)), Slice(std::to_string(eNum * 2)),
                                  0, &bufs[0]);
int replace_start = i;
int replace_end = std::min(i + 10, eNum - 1);
int range_size = replace_end - replace_start + 1;
ASSERT_EQ(base_entry
.

GetNumMiniRuns(), eNum

);
Status s = LeafIndexEntryBuilder::ReplaceMiniRunRange(base_entry, replace_start, replace_end, e, &new_entry_bufs[0],
                                                      &new_entry);
ASSERT_TRUE(s
.

ok()

);

auto entries = new_entry.GetAllMiniRunIndexEntry(LeafIndexEntry::TraversalOrder::forward);
ASSERT_EQ(entries
.

size(), eNum

- range_size + 1);
ASSERT_EQ(entries[i]
.

GetRunNumberWithinSegment(), eNum

* 2);
ASSERT_EQ(entries[i]
.

GetFilterData()

.

ToString(), std::to_string(eNum * 2)

);
ASSERT_EQ(entries[i]
.

GetBlockIndexData()

.

ToString(), std::to_string(eNum * 2)

);
}
}

{
for (
int i = 0;
i<eNum;
++i) {
bufs[0].

clear();

new_entry_bufs[0].

clear();

auto e = MiniRunIndexEntry::Build(eNum * 2, eNum * 2, Slice(std::to_string(eNum * 2)), Slice(std::to_string(eNum * 2)),
                                  0, &bufs[0]);
int remove_start = i;
int remove_end = std::min(i + 10, eNum - 1);
int range_size = remove_end - remove_start + 1;
ASSERT_EQ(base_entry
.

GetNumMiniRuns(), eNum

);
Status s = LeafIndexEntryBuilder::RemoveMiniRunRange(base_entry, remove_start, remove_end, &new_entry_bufs[0],
                                                     &new_entry);
ASSERT_TRUE(s
.

ok()

);

auto entries = new_entry.GetAllMiniRunIndexEntry(LeafIndexEntry::TraversalOrder::forward);
ASSERT_EQ(entries
.

size(), eNum

- range_size);
if (i + range_size<eNum) {
ASSERT_EQ(entries[i]
.

GetRunNumberWithinSegment(), i

+ range_size);
ASSERT_EQ(entries[i]
.

GetFilterData()

.

ToString(), std::to_string(i + range_size)

);
ASSERT_EQ(entries[i]
.

GetBlockIndexData()

.

ToString(), std::to_string(i + range_size)

);
}
}
}
}
}

}
}  // namespace leveldb



int main(int argc, char **argv) {
    return leveldb::test::RunAllTests();
}
