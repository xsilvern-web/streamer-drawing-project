const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');

const html = fs.readFileSync(`${__dirname}/../draw.html`, 'utf8');
const handlerStart = html.indexOf("document.getElementById('image-load-file').addEventListener('change', async (e) => {");
const handlerEnd = html.indexOf('async function applyTransform(', handlerStart);
assert.ok(handlerStart >= 0 && handlerEnd > handlerStart, 'Find the actual image import handler');
const handlerSource = html.slice(handlerStart, handlerEnd);

// Includes the long pauses and 40ms frames from the reported 30-frame GIF.
const reportedDurations = [
    2000, 670, 660, 670, 670, 660, 670, 670, 660, 670,
    670, 660, 670, 670, 660, 670, 670, 660, 670, 670,
    660, 670, 3000, 5000, 100, 40, 40, 40, 40, 2000
];

function createImportHarness({ supported = true, failDecodeAt, failDrawAt } = {}) {
    let importHandler;
    let resolveRead;
    let rejectRead;
    let resolveMetadata;
    let rejectMetadata;
    let readCalls = 0;
    let streamCalls = 0;
    let decoderCount = 0;
    let decoderCloseCount = 0;
    let uidCounter = 0;
    const buffer = new ArrayBuffer(16);
    const reading = new Promise((resolve, reject) => {
        resolveRead = resolve;
        rejectRead = reject;
    });
    const metadataReady = new Promise((resolve, reject) => {
        resolveMetadata = resolve;
        rejectMetadata = reject;
    });
    const track = { frameCount: 2 };
    const decodeCalls = [];
    const decodedImages = [];
    const alerts = [];
    const errors = [];
    const finishedGifGroups = [];
    const animationCheckbox = { checked: false, dispatchEvent() {} };
    const input = {
        value: String.raw`C:\fakepath\animation.gif`,
        files: [{
            type: 'image/gif',
            arrayBuffer() { readCalls++; return reading; },
            stream() { streamCalls++; throw new Error('GIF import must use the complete file buffer'); }
        }]
    };
    const frames = [{ id: 'initial', duration: 500, layers: [] }];

    class FakeImageDecoder {
        constructor(options) {
            decoderCount++;
            assert.equal(options.data, buffer, 'Pass the exact complete file buffer to the decoder');
            assert.equal(options.type, 'image/gif');
            this.tracks = { ready: metadataReady, selectedTrack: track };
            // BufferSource input is already complete, but its track metadata is asynchronous.
            this.completed = Promise.resolve();
        }

        async decode({ frameIndex }) {
            decodeCalls.push(frameIndex);
            if (frameIndex === failDecodeAt) throw new Error('decode failed');
            const image = {
                frameIndex,
                displayWidth: 1210,
                displayHeight: 738,
                duration: reportedDurations[frameIndex] * 1000,
                closeCount: 0,
                close() { this.closeCount++; }
            };
            decodedImages.push(image);
            return { image };
        }

        close() { decoderCloseCount++; }
    }

    const context = {
        window: supported ? { ImageDecoder: FakeImageDecoder } : {},
        ImageDecoder: FakeImageDecoder,
        Event: class { constructor(type) { this.type = type; } },
        CANVAS_WIDTH: 1920,
        CANVAS_HEIGHT: 1080,
        frames,
        currentFrameId: 'initial',
        layers: frames[0].layers,
        console: { error: (...args) => errors.push(args) },
        alert: message => alerts.push(message),
        // Preserve the async boundary without waiting for the legacy 2ms delay.
        setTimeout: callback => { callback(); },
        renderFrameUI() {},
        renderLayerUI() {},
        uid: prefix => `${prefix}_${uidCounter++}`,
        finishGifImport: groupId => finishedGifGroups.push(groupId),
        createNewFrame() {
            const frame = { id: `frame_${frames.length}`, duration: 500, layers: [] };
            frames.push(frame);
            context.currentFrameId = frame.id;
            context.layers = frame.layers;
        },
        createTransformObject(type, x, y, width, height, content) {
            const wrapper = { type, x, y, width, height, content };
            context.layers.push({ element: wrapper });
            return wrapper;
        },
        document: {
            getElementById(id) {
                if (id === 'image-load-file') {
                    return { addEventListener: (event, handler) => {
                        assert.equal(event, 'change');
                        importHandler = handler;
                    } };
                }
                if (id === 'chk-animation-mode') return animationCheckbox;
                throw new Error(`Unexpected element: ${id}`);
            },
            createElement(tag) {
                assert.equal(tag, 'canvas');
                return { width: 0, height: 0, getContext: () => ({
                    drawImage(image) {
                        if (failDrawAt !== undefined && image.frameIndex === failDrawAt) throw new Error('draw failed');
                    }
                }) };
            }
        }
    };
    vm.runInNewContext(handlerSource, context, { filename: 'draw.html:image-import' });

    return {
        input, frames, decodeCalls, decodedImages, alerts, errors, animationCheckbox, finishedGifGroups,
        import: () => importHandler({ target: input }),
        finishReading() { resolveRead(buffer); },
        failReading: rejectRead,
        finishMetadata() {
            track.frameCount = reportedDurations.length;
            resolveMetadata();
        },
        failMetadata: rejectMetadata,
        get readCalls() { return readCalls; },
        get streamCalls() { return streamCalls; },
        get decoderCount() { return decoderCount; },
        get decoderCloseCount() { return decoderCloseCount; }
    };
}

test('reads the entire GIF before creating the decoder and waits for metadata before importing all 30 frames', async () => {
    const harness = createImportHarness();
    const importing = harness.import();
    await Promise.resolve();
    await Promise.resolve();
    const beforeRead = {
        decoders: harness.decoderCount,
        decoded: harness.decodeCalls.length,
        frames: harness.frames.length,
        layers: harness.frames.reduce((count, frame) => count + frame.layers.length, 0)
    };

    harness.finishReading();
    await Promise.resolve();
    await Promise.resolve();
    const beforeMetadata = {
        decoders: harness.decoderCount,
        decoded: harness.decodeCalls.length,
        frames: harness.frames.length
    };
    harness.finishMetadata();
    await importing;

    assert.deepEqual(beforeRead, { decoders: 0, decoded: 0, frames: 1, layers: 0 });
    assert.deepEqual(beforeMetadata, { decoders: 1, decoded: 0, frames: 1 });
    assert.equal(harness.readCalls, 1);
    assert.equal(harness.streamCalls, 0);
    assert.equal(harness.frames.length, 30);
    assert.deepEqual(harness.decodeCalls, Array.from({ length: 30 }, (_, index) => index));
    assert.deepEqual(harness.frames.map(frame => frame.duration), reportedDurations);
    assert.deepEqual(harness.frames.map(frame => frame.layers[0].name),
        Array.from({ length: 30 }, (_, index) => `GIF 프레임 ${index + 1}`));
    assert.equal(harness.finishedGifGroups.length, 1);
    const groupId = harness.finishedGifGroups[0];
    assert.ok(groupId, 'Finish the imported GIF group once');
    assert.ok(harness.frames.every(frame => frame.layers[0].gifGroupId === groupId));
    assert.ok(harness.frames.every(frame => frame.layers[0].gifGroupLinked === true));
    assert.ok(harness.decodedImages.every(image => image.closeCount === 1));
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.animationCheckbox.checked, true);
    assert.equal(harness.errors.length, 0);
    assert.ok(harness.alerts.some(message => message.includes('(30프레임)')));
});

test('resets the input without creating a decoder when reading the file fails', async () => {
    const harness = createImportHarness();
    const importing = harness.import();
    harness.failReading(new Error('file read failed'));
    await importing;

    assert.equal(harness.decodeCalls.length, 0);
    assert.equal(harness.frames.length, 1);
    assert.equal(harness.decoderCount, 0);
    assert.equal(harness.decoderCloseCount, 0);
    assert.equal(harness.input.value, '');
    assert.equal(harness.errors.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('오류')));
});

test('releases the decoder and resets the input when track metadata fails', async () => {
    const harness = createImportHarness();
    const importing = harness.import();
    harness.finishReading();
    await Promise.resolve();
    harness.failMetadata(new Error('metadata failed'));
    await importing;

    assert.equal(harness.decodeCalls.length, 0);
    assert.equal(harness.frames.length, 1);
    assert.equal(harness.decoderCount, 1);
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.errors.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('오류')));
});

test('releases the decoder and resets the input when frame decoding fails', async () => {
    const harness = createImportHarness({ failDecodeAt: 0 });
    const importing = harness.import();
    harness.finishReading();
    harness.finishMetadata();
    await importing;

    assert.equal(harness.decodedImages.length, 0);
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.errors.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('오류')));
});

test('releases an image frame even when copying it to canvas fails', async () => {
    const harness = createImportHarness({ failDrawAt: 0 });
    const importing = harness.import();
    harness.finishReading();
    harness.finishMetadata();
    await importing;

    assert.equal(harness.decodedImages.length, 1);
    assert.equal(harness.decodedImages[0].closeCount, 1);
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.errors.length, 1);
});

test('explains unsupported browsers and resets the input without decoding', async () => {
    const harness = createImportHarness({ supported: false });
    await harness.import();

    assert.equal(harness.readCalls, 0);
    assert.equal(harness.streamCalls, 0);
    assert.equal(harness.decoderCount, 0);
    assert.equal(harness.decodeCalls.length, 0);
    assert.equal(harness.decoderCloseCount, 0);
    assert.equal(harness.input.value, '');
    assert.equal(harness.frames.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('지원하지 않습니다')));
});
