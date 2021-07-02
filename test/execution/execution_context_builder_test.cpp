#include "execution/exec/execution_context_builder.h"

#include "execution/compiled_tpl_test.h"
#include "execution/exec/execution_context.h"

/** A dummy from which we can constuct null ManagedPointers */
#define DUMMY nullptr

namespace noisepage::execution::test {

class ExecutionContextBuilderTest : public TplTest {
  /** The OID with which the database OID is initialized */
  constexpr static const uint32_t DB_OID = 15721;

 public:
  ExecutionContextBuilderTest() : db_oid_{DB_OID}, output_callback_{[](byte *, uint32_t, uint32_t) {}} {}  // NOLINT

  /** @return The dummy database OID */
  catalog::db_oid_t GetDatabaseOID() const { return db_oid_; }

  /** @return The dummy execution settings */
  const exec::ExecutionSettings &GetExecutionSettings() const { return execution_settings_; }

  /** @return The dummy output callback */
  const exec::OutputCallback &GetOutputCallback() const { return output_callback_; }

 private:
  /** A dummy database OID */
  catalog::db_oid_t db_oid_;
  /** A dummy ExecutionSettings instance */
  exec::ExecutionSettings execution_settings_{};
  /** A dummy output callback */
  const exec::OutputCallback output_callback_;
};

TEST_F(ExecutionContextBuilderTest, DoesNotThrowWithAllConfigurationSpecified) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_NO_THROW(builder.Build());
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingDatabaseOID) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingTransactionContext) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingExecutionSettings) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingOutputSchema) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingOutputCallback) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingCatalogAccessor) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingMetricsManager) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithReplicationManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingReplicationManager) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithRecoveryManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

TEST_F(ExecutionContextBuilderTest, ThrowsOnMissingRecoveryManager) {
  auto builder = exec::ExecutionContextBuilder()
                     .WithDatabaseOID(GetDatabaseOID())
                     .WithTxnContext(DUMMY)
                     .WithExecutionSettings(GetExecutionSettings())
                     .WithOutputSchema(DUMMY)
                     .WithOutputCallback(GetOutputCallback())
                     .WithCatalogAccessor(DUMMY)
                     .WithMetricsManager(DUMMY)
                     .WithReplicationManager(DUMMY);
  EXPECT_THROW(builder.Build(), ExecutionException);
}

#undef DUMMY

}  // namespace noisepage::execution::test
