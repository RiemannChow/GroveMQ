// MQTTX E2E Test Suite Runner - Executes all 500+ test cases
const BasicOperationsTests = require('./mqttx-basic-operations');
const ProtocolComplianceTests = require('./mqttx-protocol-compliance');
const TopicManagementTests = require('./mqttx-topic-management');
const ConcurrencyPerformanceTests = require('./mqttx-concurrency-performance');
const SecurityAuthTests = require('./mqttx-security-auth');
const ErrorHandlingTests = require('./mqttx-error-handling');
const RealWorldScenariosTests = require('./mqttx-realworld-scenarios');

class MQTTXTestSuite {
  constructor() {
    this.allTestResults = [];
    this.startTime = Date.now();
  }

  async runAllTests() {
    console.log('🚀 Starting MQTTX Complete Test Suite (500+ test cases)');
    console.log('='.repeat(60));

    const testSuites = [
      { name: 'Basic Operations', class: BasicOperationsTests, count: 100 },
      { name: 'Protocol Compliance', class: ProtocolComplianceTests, count: 80 },
      { name: 'Topic Management', class: TopicManagementTests, count: 75 },
      { name: 'Concurrency & Performance', class: ConcurrencyPerformanceTests, count: 60 },
      { name: 'Security & Authentication', class: SecurityAuthTests, count: 50 },
      { name: 'Error Handling & Edge Cases', class: ErrorHandlingTests, count: 60 },
      { name: 'Real-World Scenarios', class: RealWorldScenariosTests, count: 75 }
    ];

    let totalTests = 0;
    let totalPassed = 0;
    let totalFailed = 0;

    for (const suite of testSuites) {
      console.log(`\n🧪 Running ${suite.name} Tests (${suite.count} test cases)`);
      console.log('-'.repeat(60));

      try {
        const testInstance = new suite.class();
        const report = await testInstance.runAllTests();

        this.allTestResults.push({
          suiteName: suite.name,
          expectedCount: suite.count,
          ...report.summary
        });

        totalTests += report.summary.total;
        totalPassed += report.summary.passed;
        totalFailed += report.summary.failed;

        console.log(`✅ ${suite.name} completed: ${report.summary.passed}/${report.summary.total} passed (${report.summary.passRate}%)`);

        // Small delay between test suites to avoid overwhelming the broker
        await this.sleep(2000);

      } catch (error) {
        console.error(`❌ ${suite.name} failed to execute:`, error.message);
        this.allTestResults.push({
          suiteName: suite.name,
          expectedCount: suite.count,
          total: 0,
          passed: 0,
          failed: suite.count,
          passRate: 0,
          error: error.message
        });
        totalFailed += suite.count;
      }
    }

    this.generateFinalReport(totalTests, totalPassed, totalFailed);
    return {
      totalTests,
      totalPassed,
      totalFailed,
      passRate: totalTests > 0 ? (totalPassed / totalTests * 100).toFixed(1) : 0,
      suiteResults: this.allTestResults
    };
  }

  generateFinalReport(totalTests, totalPassed, totalFailed) {
    const totalDuration = Date.now() - this.startTime;
    const passRate = totalTests > 0 ? (totalPassed / totalTests * 100).toFixed(1) : 0;

    console.log('\n' + '='.repeat(80));
    console.log('📊 MQTTX E2E TEST SUITE FINAL REPORT');
    console.log('='.repeat(80));
    console.log(`Total Test Cases: ${totalTests}`);
    console.log(`Passed: ${totalPassed}`);
    console.log(`Failed: ${totalFailed}`);
    console.log(`Pass Rate: ${passRate}%`);
    console.log(`Total Duration: ${(totalDuration / 1000).toFixed(2)} seconds`);
    console.log('\n📋 Test Suite Breakdown:');
    console.log('-'.repeat(80));

    for (const result of this.allTestResults) {
      const status = result.failed === 0 ? '✅' : '❌';
      console.log(`${status} ${result.suiteName.padEnd(30)} ${result.passed}/${result.total} (${result.passRate}%)`);

      if (result.error) {
        console.log(`   Error: ${result.error}`);
      }
    }

    console.log('\n🎯 Test Coverage Summary:');
    console.log('-'.repeat(80));
    console.log('✓ MQTT 3.1.1 Protocol Compliance');
    console.log('✓ MQTT 5.0 Features');
    console.log('✓ Basic Operations (Connect, Publish, Subscribe)');
    console.log('✓ QoS Levels (0, 1, 2)');
    console.log('✓ Topic Management & Wildcards');
    console.log('✓ Session Management (Clean/Persistent)');
    console.log('✓ Last Will Testament');
    console.log('✓ Concurrency & Performance');
    console.log('✓ Security & Authentication');
    console.log('✓ Error Handling & Edge Cases');
    console.log('✓ Real-World Scenarios (IoT, Smart Home, Industrial)');

    if (totalFailed > 0) {
      console.log('\n⚠️  Failed Tests Summary:');
      console.log('-'.repeat(80));
      for (const result of this.allTestResults) {
        if (result.failed > 0) {
          console.log(`❌ ${result.suiteName}: ${result.failed} failed tests`);
        }
      }
    }

    console.log('\n🔧 Next Steps:');
    console.log('-'.repeat(80));
    if (totalFailed === 0) {
      console.log('🎉 All tests passed! GroveMQ implementation is working correctly.');
    } else {
      console.log('🔍 Review failed tests and fix GroveMQ source code issues.');
      console.log('📝 Check broker configuration and network connectivity.');
      console.log('🚀 Re-run tests after making fixes.');
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Run the complete test suite if this file is executed directly
if (require.main === module) {
  (async () => {
    const testSuite = new MQTTXTestSuite();

    try {
      const finalReport = await testSuite.runAllTests();

      // Exit with appropriate code
      process.exit(finalReport.totalFailed > 0 ? 1 : 0);

    } catch (error) {
      console.error('❌ Test suite execution failed:', error);
      process.exit(1);
    }
  })();
}

module.exports = MQTTXTestSuite;