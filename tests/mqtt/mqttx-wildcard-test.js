#!/usr/bin/env node

const mqtt = require('mqtt');

console.log('🧪 MQTT通配符专项测试');
console.log('测试单级通配符(+)和多级通配符(#)');
console.log('==========================================\n');

// MQTT连接配置
const brokerUrl = 'mqtt://localhost:1883';
const clientOptions = {
    clientId: 'wildcard-test-' + Math.random().toString(16).substr(2, 8),
    username: 'test',
    password: 'test',
    protocolVersion: 4,
    clean: true
};

const client = mqtt.connect(brokerUrl, clientOptions);

let testResults = {
    singleLevel: false,
    multiLevel: false,
    nestedSingleLevel: false,
    mixedPattern: false
};

client.on('connect', () => {
    console.log('✅ 连接到 grovemq broker');
    console.log('🔧 客户端ID:', clientOptions.clientId);

    setTimeout(() => runWildcardTests(), 1000);
});

client.on('error', (error) => {
    console.error('❌ 连接错误:', error.message);
    process.exit(1);
});

client.on('message', (topic, message) => {
    const payload = message.toString();
    console.log(`📩 收到消息: ${topic} -> ${payload}`);

    // 标记测试结果
    if (payload.includes('single-level test')) testResults.singleLevel = true;
    if (payload.includes('multi-level test')) testResults.multiLevel = true;
    if (payload.includes('nested single-level test')) testResults.nestedSingleLevel = true;
    if (payload.includes('mixed pattern test')) testResults.mixedPattern = true;
});

async function runWildcardTests() {
    console.log('🔸 测试1: 单级通配符 (+)');

    // 订阅 sensor/+/temperature
    client.subscribe('sensor/+/temperature', (error) => {
        if (error) {
            console.error('❌ 订阅失败:', error);
        } else {
            console.log('  ✓ 订阅: sensor/+/temperature');
        }
    });

    setTimeout(() => {
        // 发布匹配的消息
        client.publish('sensor/room1/temperature', 'single-level test message', (error) => {
            if (!error) {
                console.log('  ✓ 发布: sensor/room1/temperature');
            }
        });
    }, 500);

    setTimeout(() => testMultiLevel(), 2000);
}

function testMultiLevel() {
    console.log('\n🔸 测试2: 多级通配符 (#)');

    // 订阅 home/#
    client.subscribe('home/#', (error) => {
        if (error) {
            console.error('❌ 订阅失败:', error);
        } else {
            console.log('  ✓ 订阅: home/#');
        }
    });

    setTimeout(() => {
        // 发布多个层级的消息
        const topics = [
            'home/living-room/light',
            'home/kitchen/temperature',
            'home/bedroom/door/status'
        ];

        topics.forEach((topic, index) => {
            setTimeout(() => {
                client.publish(topic, 'multi-level test message', (error) => {
                    if (!error) {
                        console.log(`  ✓ 发布: ${topic}`);
                    }
                });
            }, index * 200);
        });
    }, 500);

    setTimeout(() => testNestedSingleLevel(), 3000);
}

function testNestedSingleLevel() {
    console.log('\n🔸 测试3: 嵌套单级通配符 (+/+)');

    // 订阅 device/+/+
    client.subscribe('device/+/+', (error) => {
        if (error) {
            console.error('❌ 订阅失败:', error);
        } else {
            console.log('  ✓ 订阅: device/+/+');
        }
    });

    setTimeout(() => {
        client.publish('device/sensor/001', 'nested single-level test message', (error) => {
            if (!error) {
                console.log('  ✓ 发布: device/sensor/001');
            }
        });
    }, 500);

    setTimeout(() => testMixedPattern(), 2000);
}

function testMixedPattern() {
    console.log('\n🔸 测试4: 混合通配符模式 (+/#)');

    // 订阅 building/+/#
    client.subscribe('building/+/#', (error) => {
        if (error) {
            console.error('❌ 订阅失败:', error);
        } else {
            console.log('  ✓ 订阅: building/+/#');
        }
    });

    setTimeout(() => {
        client.publish('building/floor1/room/sensor/temp', 'mixed pattern test message', (error) => {
            if (!error) {
                console.log('  ✓ 发布: building/floor1/room/sensor/temp');
            }
        });
    }, 500);

    setTimeout(() => showResults(), 2000);
}

function showResults() {
    console.log('\n🏁 通配符测试结果:');
    console.log('================================');
    console.log('单级通配符 (+):     ', testResults.singleLevel ? '✅ 成功' : '❌ 失败');
    console.log('多级通配符 (#):     ', testResults.multiLevel ? '✅ 成功' : '❌ 失败');
    console.log('嵌套单级通配符 (+/+):', testResults.nestedSingleLevel ? '✅ 成功' : '❌ 失败');
    console.log('混合通配符 (+/#):   ', testResults.mixedPattern ? '✅ 成功' : '❌ 失败');
    console.log('================================');

    const successCount = Object.values(testResults).filter(result => result).length;
    const totalTests = Object.keys(testResults).length;

    console.log(`📊 通配符测试通过率: ${successCount}/${totalTests} (${Math.round(successCount/totalTests*100)}%)`);

    setTimeout(() => {
        client.end();
        console.log('\n👋 通配符测试完成，已断开连接');
        process.exit(0);
    }, 1000);
}