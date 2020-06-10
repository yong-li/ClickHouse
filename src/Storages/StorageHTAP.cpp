#include <Common/Exception.h>

#include <DataStreams/IBlockOutputStream.h>

#include <Parsers/ASTCreateQuery.h>

#include <Storages/StorageHTAP.h>
#include <Storages/StorageFactory.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>

namespace DB {

namespace ErrorCodes {
extern const int STORAGE_REQUIRES_PARAMETER;
}

class TableReader : public SourceWithProgress {
  public:
    TableReader(const Names& column_names_, Table::iterator begin_, Table::iterator end_,
                StorageHTAP* storage_)
        : SourceWithProgress{storage_->getSampleBlockForColumns(column_names_)},
          column_names{column_names_}, begin{begin_}, end{end_}, storage(storage_) {}

    String getName() const override { return "Table"; }

  protected:
    Chunk generate() override {
        if (begin == end)
            return {};

        std::lock_guard lock(storage->mutex);

        MutableColumns columns;
        std::vector<uint32_t> column_positions;
        columns.reserve(column_names.size());
        column_positions.reserve(column_names.size());

        for (const std::string& col_name : column_names) {
            uint32_t pos = 0;
            for (const auto& col_name_type : storage->getColumns().getAllPhysical()) {
                if (col_name == col_name_type.name) {
                    columns.push_back(col_name_type.type->createColumn());
                    column_positions.push_back(pos);
                    break;
                }
                pos++;
            }
        }

        uint32_t row_count = 0;
        while (begin != end) {
            const Tuple& row = begin->second;
            for (size_t i = 0; i < columns.size(); i++) {
                columns[i]->insert(row[column_positions[i]]);
            }
            row_count++;
            begin++;
        }

        return Chunk(std::move(columns), row_count);
    }

  private:
    Names column_names;
    Table::iterator begin;
    Table::iterator end;
    StorageHTAP* storage;
};

class TableWriter : public IBlockOutputStream {
  public:
    explicit TableWriter(StorageHTAP* storage_) : storage{storage_} {}

    Block getHeader() const override {
        return storage->getSampleBlock();
    }

    void write(const Block& block) override {
        Logger* logger = &Logger::get("TableWriter");

        LOG_INFO(logger, "Writing HTAP table.");

        storage->check(block, true);
        std::lock_guard lock(storage->mutex);

        Columns pk_cols;
        for (const auto& pk_col_name : storage->getPrimaryKeyColumns()) {
            LOG_INFO(logger, "Primary key column: {}", pk_col_name);
            pk_cols.push_back(block.getByName(pk_col_name).column);
        }

        Columns cols;
        for (const auto& col_name_type : storage->getColumns().getAllPhysical()) {
            LOG_INFO(logger, "Column: {}", col_name_type.name);
            cols.push_back(block.getByName(col_name_type.name).column);
        }

        for (size_t i = 0; i < block.rows(); i++) {
            Tuple key;
            for (auto& column : pk_cols) {
                key.push_back((*column)[i]);
            }
            Tuple row;
            for (auto& column : cols) {
                row.push_back((*column)[i]);
            }
            storage->table.insert_or_assign(key, row);
        }

    }

  private:
    StorageHTAP* storage;
};

StorageHTAP::StorageHTAP(const StorageFactory::Arguments& args)
    : IStorage(args.table_id) {
    Logger* logger = &Logger::get("StorageHTAP");

    StorageFactory::Arguments copy{
        .engine_name = "VersionedCollapsingMergeTree",
        .engine_args = args.engine_args,
        .storage_def = args.storage_def,
        .query = args.query,
        .relative_data_path = args.relative_data_path,
        .table_id = args.table_id,
        .local_context = args.local_context,
        .context = args.context,
        .columns = args.columns,
        .constraints = args.constraints,
        .attach = args.attach,
        .has_force_restore_data_flag = args.has_force_restore_data_flag
    };

    const auto& storages = StorageFactory::instance().getAllStorages();
    const auto& creator = storages.at("VersionedCollapsingMergeTree");
    base_storage = std::dynamic_pointer_cast<StorageMergeTree>(creator.creator_fn(copy));

    setColumns(args.columns);
    setSortingKey(base_storage->getSortingKey());
    setPrimaryKey(base_storage->getPrimaryKey());

    for (const auto& pk_col : base_storage->getPrimaryKeyColumns()) {
        LOG_INFO(logger, "BASE Primary key column: {}.", pk_col);
    }
    for (const auto& pk_col : getPrimaryKeyColumns()) {
        LOG_INFO(logger, "HTAP Primary key column: {}.", pk_col);
    }
}

Pipes StorageHTAP::read(const Names& column_names,
                        const SelectQueryInfo&,
                        const Context&,
                        QueryProcessingStage::Enum,
                        size_t,
                        unsigned) {
    check(column_names);

    std::lock_guard lock(mutex);

    Pipes pipes;
    pipes.emplace_back(std::make_shared<TableReader>(column_names,
                                                     table.begin(),
                                                     table.end(),
                                                     this));
    return pipes;
}

BlockOutputStreamPtr StorageHTAP::write(const ASTPtr&,
                                        const Context&) {
    Logger* logger = &Logger::get("StorageHTAP");

    for (const auto& pk_col : getPrimaryKeyColumns()) {
        LOG_INFO(logger, "On write() Primary key column: {}.", pk_col);
    }

    return std::make_shared<TableWriter>(this);
}

bool StorageHTAP::optimize(const DB::ASTPtr& query,
                           const DB::ASTPtr& partition,
                           bool,
                           bool,
                           const DB::Context& context) {
    base_storage->optimize(query, partition, true, true, context);
}

void StorageHTAP::drop() {
    std::lock_guard lock(mutex);
    table.clear();
    base_storage->drop();
}

void StorageHTAP::truncate(const ASTPtr& astPtr,
                           const Context& context,
                           TableStructureWriteLockHolder& lockHolder) {
    std::lock_guard lock(mutex);
    table.clear();
    base_storage->truncate(astPtr, context, lockHolder);
}

std::optional<UInt64> StorageHTAP::totalRows() const {
    std::lock_guard lock(mutex);
    return table.size();
}

static StoragePtr createHTAP(const StorageFactory::Arguments& args) {
    Logger* logger = &Logger::get("StorageHTAP");

    LOG_INFO(logger, "Create an HTAP table '{}'.", args.table_id.getFullTableName());

    if (args.storage_def->primary_key == nullptr) {
        throw Exception("Primary key must be defined for HTAP tables.",
                        ErrorCodes::STORAGE_REQUIRES_PARAMETER);
    }

    return StorageHTAP::create(args);
}

void registerStorageHTAP(StorageFactory& factory) {
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_sort_order = true,
        .supports_ttl = true,
    };

    factory.registerStorage("HTAP", createHTAP, features);
}

}