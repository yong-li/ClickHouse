
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/StorageHTAP.h>
#include <Storages/StorageFactory.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>


namespace DB {

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
        auto it = begin;
        while (it != end) {
            const Tuple& row = it->second;
            for (size_t i = 0; i < columns.size(); i++) {
                columns[i]->insert(row[column_positions[i]]);
            }
            row_count++;
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
        storage->check(block, true);
        std::lock_guard lock(storage->mutex);

        Columns pk_cols;
        for (const auto& pk_col_name : storage->getPrimaryKeyColumns()) {
            pk_cols.push_back(block.getByName(pk_col_name).column);
        }

        Columns cols;
        for (const auto& col_name_type : storage->getColumns().getAllPhysical()) {
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
    return std::make_shared<TableWriter>(this);
}

void StorageHTAP::drop() {
    std::lock_guard lock(mutex);
    table.clear();
}

void StorageHTAP::truncate(const ASTPtr&,
                           const Context&,
                           TableStructureWriteLockHolder&) {
    std::lock_guard lock(mutex);
    table.clear();
}

std::optional<UInt64> StorageHTAP::totalRows() const {
    std::lock_guard lock(mutex);
    return table.size();
}

}