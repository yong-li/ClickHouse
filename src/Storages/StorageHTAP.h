#pragma once

#include <map>
#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>


namespace DB
{

using Table = std::map<Tuple, Tuple>;

class StorageHTAP final : public ext::shared_ptr_helper<StorageHTAP>, public IStorage {
  public:
    String getName() const override { return "HTAP"; }

    Pipes read(
        const Names& column_names,
        const SelectQueryInfo& query_info,
        const Context& context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr& query, const Context& context) override;

    void drop() override;

    void truncate(const ASTPtr&, const Context&, TableStructureWriteLockHolder&) override;

    std::optional<UInt64> totalRows() const override;
    std::optional<UInt64> totalBytes() const override { return 1024UL; }

    mutable std::mutex mutex;

    Table table;
};

}
