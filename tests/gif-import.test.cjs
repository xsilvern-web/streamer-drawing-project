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
    let resolveCompleted;
    let rejectCompleted;
    let decoderCloseCount = 0;
    const completed = new Promise((resolve, reject) => {
        resolveCompleted = resolve;
        rejectCompleted = reject;
    });
    const track = { frameCount: 2 };
    const decodeCalls = [];
    const decodedImages = [];
    const alerts = [];
    const errors = [];
    const animationCheckbox = { checked: false, dispatchEvent() {} };
    const stream = {};
    const input = {
        value: String.raw`C:\fakepath\animation.gif`,
        files: [{ type: 'image/gif', stream: () => stream }]
    };
    const frames = [{ id: 'initial', duration: 500, layers: [] }];

    class FakeImageDecoder {
        constructor(options) {
            assert.equal(options.data, stream);
            assert.equal(options.type, 'image/gif');
            this.tracks = { ready: Promise.resolve(), selectedTrack: track };
            this.completed = completed;
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
        input, frames, decodeCalls, decodedImages, alerts, errors, animationCheckbox,
        import: () => importHandler({ target: input }),
        finishBuffering() {
            track.frameCount = reportedDurations.length;
            resolveCompleted();
        },
        failBuffering: rejectCompleted,
        get decoderCloseCount() { return decoderCloseCount; }
    };
}

test('waits for the complete GIF before reading its frame count and imports all 30 frames', async () => {
    const harness = createImportHarness();
    const importing = harness.import();
    await Promise.resolve();
    await Promise.resolve();
    const beforeComplete = {
        decoded: harness.decodeCalls.length,
        frames: harness.frames.length,
        layers: harness.frames.reduce((count, frame) => count + frame.layers.length, 0)
    };

    harness.finishBuffering();
    await importing;

    assert.deepEqual(beforeComplete, { decoded: 0, frames: 1, layers: 0 });
    assert.equal(harness.frames.length, 30);
    assert.deepEqual(harness.decodeCalls, Array.from({ length: 30 }, (_, index) => index));
    assert.deepEqual(harness.frames.map(frame => frame.duration), reportedDurations);
    assert.deepEqual(harness.frames.map(frame => frame.layers[0].name),
        Array.from({ length: 30 }, (_, index) => `GIF 프레임 ${index + 1}`));
    assert.ok(harness.decodedImages.every(image => image.closeCount === 1));
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.animationCheckbox.checked, true);
    assert.equal(harness.errors.length, 0);
    assert.ok(harness.alerts.some(message => message.includes('(30프레임)')));
});

test('releases the decoder and resets the input when buffering fails', async () => {
    const harness = createImportHarness();
    const importing = harness.import();
    harness.failBuffering(new Error('stream failed'));
    await importing;

    assert.equal(harness.decodeCalls.length, 0);
    assert.equal(harness.frames.length, 1);
    assert.equal(harness.decoderCloseCount, 1);
    assert.equal(harness.input.value, '');
    assert.equal(harness.errors.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('오류')));
});

test('releases the decoder and resets the input when frame decoding fails', async () => {
    const harness = createImportHarness({ failDecodeAt: 0 });
    const importing = harness.import();
    harness.finishBuffering();
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
    harness.finishBuffering();
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

    assert.equal(harness.decodeCalls.length, 0);
    assert.equal(harness.decoderCloseCount, 0);
    assert.equal(harness.input.value, '');
    assert.equal(harness.frames.length, 1);
    assert.ok(harness.alerts.some(message => message.includes('지원하지 않습니다')));
});
