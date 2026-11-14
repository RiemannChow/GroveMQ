#!/usr/bin/env node

const mqtt = require('mqtt');

// MQTT测试配置
const brokerUrl = 'mqtt://localhost:1883';
const clientOptions = {
    clientId: 'mqttx-test-client-' + Math.random().toString(16).substr(2, 8),
    username: 'test',
    password: 'test',
    protocolVersion: 4,
    clean: true
};

console.log('🚀 开始MQTT消息收发测试');
console.log('📡 连接到 grovemq broker:', brokerUrl);

// 测试主题
const testTopics = {
    basic: 'mqttx/test/basic',
    qos: 'mqttx/test/qos',
    retained: 'mqttx/test/retained',
    wildcard: 'mqttx/test/+/wildcard'
};

// 测试计数器
let testResults = {
    basic: false,
    qos0: false,
    qos1: false,
    qos2: false,
    retained: false,
    wildcard: false
};

// 创建MQTT客户端
const client = mqtt.connect(brokerUrl, clientOptions);

client.on('connect', () => {
    console.log('✅ 成功连接到 grovemq broker');
    console.log('🔧 客户端ID:', clientOptions.clientId);

    // 开始测试序列
    setTimeout(() => runTests(), 1000);
});

client.on('error', (error) => {
    console.error('❌ 连接错误:', error.message);
    process.exit(1);
});

client.on('message', (topic, message) => {
    const payload = message.toString();
    console.log(`📩 收到消息: ${topic} -> ${payload}`);

    // 根据消息内容标记测试结果
    if (payload.includes('basic test')) testResults.basic = true;
    if (payload.includes('QoS 0')) testResults.qos0 = true;
    if (payload.includes('QoS 1')) testResults.qos1 = true;
    if (payload.includes('QoS 2')) testResults.qos2 = true;
    if (payload.includes('retained')) testResults.retained = true;
    if (payload.includes('wildcard')) testResults.wildcard = true;
});

function runTests() {
    console.log('\n🧪 开始执行MQTT功能测试...\n');

    // 测试1: 基本发布订阅
    test1_BasicPubSub();
}

function test1_BasicPubSub() {
    console.log('🔸 测试1: 基本发布/订阅功能');

    client.subscribe(testTopics.basic, (error) => {
        if (error) {
            console.error('❌ 订阅失败:', error);
            return;
        }
        console.log('  ✓ 订阅主题:', testTopics.basic);

        // 发布消息
        setTimeout(() => {
            client.publish(testTopics.basic, 'Hello from MQTTX test - basic test message', (error) => {
                if (error) {
                    console.error('❌ 发布失败:', error);
                } else {
                    console.log('  ✓ 发布消息到:', testTopics.basic);
                }
            });
        }, 500);
    });

    // 继续下一个测试
    setTimeout(() => test2_QoSLevels(), 2000);
}

function test2_QoSLevels() {
    console.log('\n🔸 测试2: 不同QoS级别');

    const qosTopics = ['mqttx/test/qos0', 'mqttx/test/qos1', 'mqttx/test/qos2'];

    // 订阅QoS测试主题
    qosTopics.forEach((topic, index) => {
        client.subscribe(topic, index, (error) => {
            if (error) {
                console.error(`❌ QoS ${index} 订阅失败:`, error);
            } else {
                console.log(`  ✓ 订阅 QoS ${index} 主题:`, topic);
            }
        });
    });

    // 发布不同QoS级别的消息
    setTimeout(() => {
        qosTopics.forEach((topic, index) => {
            const message = `QoS ${index} test message`;
            client.publish(topic, message, { qos: index }, (error) => {
                if (error) {
                    console.error(`❌ QoS ${index} 发布失败:`, error);
                } else {
                    console.log(`  ✓ 发布 QoS ${index} 消息到:`, topic);
                }
            });
        });
    }, 1000);

    // 继续下一个测试
    setTimeout(() => test3_RetainedMessages(), 4000);
}

function test3_RetainedMessages() {
    console.log('\n🔸 测试3: 保留消息功能');

    // 发布保留消息
    client.publish(testTopics.retained, 'This is a retained message', { retain: true }, (error) => {
        if (error) {
            console.error('❌ 保留消息发布失败:', error);
        } else {
            console.log('  ✓ 发布保留消息到:', testTopics.retained);
        }
    });

    // 等待一下，然后订阅以测试保留消息
    setTimeout(() => {
        client.subscribe(testTopics.retained, (error) => {
            if (error) {
                console.error('❌ 保留消息订阅失败:', error);
            } else {
                console.log('  ✓ 订阅保留消息主题:', testTopics.retained);
            }
        });
    }, 1000);

    // 继续下一个测试
    setTimeout(() => test4_WildcardSubscription(), 3000);
}

function test4_WildcardSubscription() {
    console.log('\n🔸 测试4: 通配符订阅');

    // 订阅通配符主题
    client.subscribe(testTopics.wildcard, (error) => {
        if (error) {
            console.error('❌ 通配符订阅失败:', error);
        } else {
            console.log('  ✓ 订阅通配符主题:', testTopics.wildcard);
        }
    });

    // 发布到匹配通配符的主题
    setTimeout(() => {
        const matchingTopic = 'mqttx/test/sensor1/wildcard';
        client.publish(matchingTopic, 'Wildcard test message', (error) => {
            if (error) {
                console.error('❌ 通配符消息发布失败:', error);
            } else {
                console.log('  ✓ 发布消息到通配符匹配主题:', matchingTopic);
            }
        });
    }, 1000);

    // 完成测试
    setTimeout(() => finishTests(), 3000);
}

function finishTests() {
    console.log('\n🏁 测试完成! 结果总结:');
    console.log('================================');
    console.log('基本发布订阅:', testResults.basic ? '✅ 成功' : '❌ 失败');
    console.log('QoS 0 消息:  ', testResults.qos0 ? '✅ 成功' : '❌ 失败');
    console.log('QoS 1 消息:  ', testResults.qos1 ? '✅ 成功' : '❌ 失败');
    console.log('QoS 2 消息:  ', testResults.qos2 ? '✅ 成功' : '❌ 失败');
    console.log('保留消息:    ', testResults.retained ? '✅ 成功' : '❌ 失败');
    console.log('通配符订阅:  ', testResults.wildcard ? '✅ 成功' : '❌ 失败');
    console.log('================================');

    const successCount = Object.values(testResults).filter(result => result).length;
    const totalTests = Object.keys(testResults).length;

    console.log(`📊 测试通过率: ${successCount}/${totalTests} (${Math.round(successCount/totalTests*100)}%)`);

    // 断开连接
    setTimeout(() => {
        client.end();
        console.log('\n👋 测试完成，已断开连接');
        process.exit(0);
    }, 1000);
}