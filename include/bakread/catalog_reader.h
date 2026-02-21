#pragma once

#include "bakread/page.h"
#include "bakread/types.h"

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// CatalogReader -- reads SQL Server system catalog from raw pages
//
// SQL Server stores metadata in system base tables (visible via DAC):
//   sys.sysschobjs   -> object_id, schema_id, name, type
//   sys.syscolpars    -> column definitions
//   sys.sysscalartypes -> type mappings
//   sys.sysidxstats   -> index metadata
//   sys.sysrowsets    -> partition/hobt info
//   sys.sysallocunits -> allocation unit mapping
//
// These system tables live on specific pages (typically early in file 1).
// The boot page (page 9 of file 1) contains the root page IDs for
// system base tables.
//
// This reader scans pages looking for system table data to resolve
// the schema of a user-specified table.
// -------------------------------------------------------------------------

struct SystemObject {
    int32_t     object_id  = 0;
    int32_t     schema_id  = 0;
    std::string name;
    std::string type;       // "U " = user table, "S " = system table
};

struct SystemColumn {
    int32_t     object_id  = 0;
    int32_t     column_id  = 0;
    std::string name;
    uint8_t     system_type_id = 0;
    int16_t     max_length = 0;
    uint8_t     precision  = 0;
    uint8_t     scale      = 0;
    bool        is_nullable = true;
    bool        is_identity = false;
    int32_t     leaf_offset = 0;
};

struct SystemIndex {
    int32_t object_id = 0;
    int32_t index_id  = 0;
    std::string name;
    uint8_t type      = 0;  // 0=heap, 1=clustered, 2=nonclustered
};

// Module definition (stored procedures, functions, views)
struct SystemModule {
    int32_t     object_id  = 0;
    int32_t     schema_id  = 0;
    std::string schema_name;
    std::string name;
    std::string type;       // "P " = procedure, "FN" = scalar function, "IF" = inline TVF, "TF" = TVF, "V " = view
    std::string definition; // The T-SQL definition text
};

// Database principal (users, roles)
struct SystemPrincipal {
    int32_t     principal_id = 0;
    std::string name;
    std::string type;       // "S" = SQL user, "U" = Windows user, "R" = role, "A" = application role
    int32_t     owning_principal_id = 0;
    std::string default_schema;
    bool        is_fixed_role = false;
};

// Database role membership
struct SystemRoleMember {
    int32_t role_principal_id   = 0;
    int32_t member_principal_id = 0;
    std::string role_name;
    std::string member_name;
};

// Database permission
struct SystemPermission {
    int32_t     class_type      = 0;    // 0=database, 1=object, 3=schema
    int32_t     major_id        = 0;    // object_id or schema_id
    int32_t     minor_id        = 0;    // column_id (0 if N/A)
    int32_t     grantee_id      = 0;    // principal who has permission
    int32_t     grantor_id      = 0;    // principal who granted
    std::string type;                    // permission type code
    std::string permission_name;         // readable name (SELECT, EXECUTE, etc.)
    std::string state;                   // G=GRANT, D=DENY, R=REVOKE, W=GRANT_WITH_GRANT
    std::string grantee_name;
    std::string grantor_name;
    std::string object_name;             // name of the object (if applicable)
    std::string schema_name;             // schema of the object (if applicable)
};

struct SystemAllocationUnit {
    int64_t allocation_unit_id = 0;
    int64_t container_id       = 0;  // hobt_id or partition_id
    int32_t type               = 0;
    PageId  first_page;
    PageId  root_page;
    PageId  first_iam_page;
};

using PageProvider = std::function<bool(int32_t file_id, int32_t page_id,
                                        uint8_t* page_buf)>;

class CatalogReader {
public:
    explicit CatalogReader(PageProvider provider);

    // Scan system catalog pages to build metadata.
    // This reads the boot page and follows references to system tables.
    bool scan_catalog();

    // Resolve a table by schema + name
    bool resolve_table(const std::string& schema_name,
                       const std::string& table_name,
                       TableSchema& out_schema) const;

    // Get all user tables found in the catalog
    std::vector<SystemObject> list_user_tables() const;

    // Get all modules (stored procedures, functions, views)
    std::vector<SystemModule> list_modules() const;

    // Get all database principals (users, roles)
    std::vector<SystemPrincipal> list_principals() const;

    // Get all role memberships
    std::vector<SystemRoleMember> list_role_members() const;

    // Get all database permissions
    std::vector<SystemPermission> list_permissions() const;

    // Get allocation info for a table (which pages contain its data)
    std::vector<SystemAllocationUnit> get_allocation_units(int32_t object_id) const;

    // Get IAM chain for a specific allocation unit
    std::vector<PageId> get_iam_chain(const PageId& first_iam) const;

    // Get the page header m_objId value for a given object_id.
    // Returns 0 if not found.
    uint32_t get_page_obj_id(int32_t object_id) const;

private:
    bool read_boot_page();
    bool scan_system_objects();
    bool scan_system_columns();
    bool scan_system_indexes();
    bool scan_system_allocation_units();
    bool scan_rowset_allocunit_mapping();
    bool scan_module_definitions();
    bool scan_principals();
    bool scan_role_members();
    bool scan_permissions();

    // Extract system table rows from a known set of pages
    void scan_pages_for_objects(int32_t start_page, int32_t file_id);
    void scan_pages_for_columns(int32_t start_page, int32_t file_id);

    // System schema IDs -> names (e.g., 1 -> "dbo")
    std::string schema_name_for_id(int32_t schema_id) const;

    PageProvider page_provider_;

    // Discovered system metadata
    std::unordered_map<int32_t, SystemObject>  objects_;   // by object_id
    std::unordered_map<int32_t, std::vector<SystemColumn>> columns_;  // by object_id
    std::unordered_map<int32_t, std::vector<SystemIndex>>  indexes_;  // by object_id
    std::vector<SystemAllocationUnit> alloc_units_;
    std::unordered_map<int32_t, std::string> schema_names_;  // schema_id -> name

    // Modules (procedures, functions, views)
    std::unordered_map<int32_t, SystemModule> modules_;  // by object_id

    // Security metadata
    std::unordered_map<int32_t, SystemPrincipal> principals_;  // by principal_id
    std::vector<SystemRoleMember> role_members_;
    std::vector<SystemPermission> permissions_;

    // Mapping: object_id -> page header m_objId (via sysrowsets + sysallocunits)
    std::unordered_map<int32_t, uint32_t> obj_to_page_objid_;

    // Boot page metadata
    int32_t sysschobjs_root_page_ = 0;
    int32_t syscolpars_root_page_ = 0;
    int32_t sysidxstats_root_page_ = 0;
    int32_t sysallocunits_root_page_ = 0;
    int32_t sysrowsets_root_page_ = 0;
};

}  // namespace bakread
