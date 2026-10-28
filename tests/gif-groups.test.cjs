const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');

const html = fs.readFileSync(`${__dirname}/../draw.html`, 'utf8');
const start = html.indexOf('function getGifGroupMembers(groupId)');
const end = html.indexOf("document.getElementById('gif-transform-linked').addEventListener", start);
assert.ok(start >= 0 && end > start, 'Find the actual GIF group helpers');
const helpers = html.slice(start, end);

function transformLayer(groupId, visible = true) {
    return {
        id: 'layer_2', type: 'transform_box', gifGroupId: groupId,
        gifGroupLinked: true, visible,
        element: { style: {
            left: '100px', top: '150px', width: '800px', height: '480px',
            transform: 'rotate(0deg)', display: 'none', opacity: '0.7'
        } }
    };
}

function createHarness({ failFrame } = {}) {
    const frames = ['gif-a', 'gif-a', 'gif-b', 'gif-a'].map((group, index) => ({
        id: `frame_${index}`, duration: [2000, 670, 40, 5000][index],
        layers: [{ id: 'layer_1', type: 'canvas', visible: true, canvas: { style: {} } },
            transformLayer(group, index !== 1)],
        history: [], fullPlaybackRecord: [], redoStack: [{ old: true }], activeLayerId: 'layer_2'
    }));
    const applied = [];
    const selected = [];
    const alerts = [];
    const errors = [];
    const context = {
        frames, currentFrameId: frames[0].id, activeLayerId: 'layer_2',
        layers: frames[0].layers, history: frames[0].history,
        fullPlaybackRecord: frames[0].fullPlaybackRecord, redoStack: frames[0].redoStack,
        isApplyingGifGroup: false, isRasterizing: false, isRedrawing: false, isPlaying: false,
        alert: message => alerts.push(message),
        console: { error: (...args) => errors.push(args) },
        updateOnionSkin() {},
        syncToFrame() {
            const frame = frames.find(frame => frame.id === context.currentFrameId);
            Object.assign(frame, {
                layers: context.layers, history: context.history,
                fullPlaybackRecord: context.fullPlaybackRecord,
                redoStack: context.redoStack, activeLayerId: context.activeLayerId
            });
        },
        selectFrame(id) {
            if (id === context.currentFrameId) return;
            context.syncToFrame();
            for (const layer of context.layers) {
                (layer.element || layer.canvas).style.display = 'none';
            }
            const frame = frames.find(frame => frame.id === id);
            context.currentFrameId = id;
            context.layers = frame.layers;
            context.history = frame.history;
            context.fullPlaybackRecord = frame.fullPlaybackRecord;
            context.redoStack = frame.redoStack;
            context.activeLayerId = frame.activeLayerId;
            for (const layer of frame.layers) {
                (layer.element || layer.canvas).style.display = layer.visible
                    ? (layer.element ? 'flex' : 'block') : 'none';
            }
            selected.push(id);
        },
        selectLayer(id) { context.activeLayerId = id; },
        async applyTransform(id) {
            if (context.currentFrameId === failFrame) throw new Error('rasterization failed');
            const layer = context.layers.find(layer => layer.id === id);
            assert.equal(layer.type, 'transform_box');
            assert.equal(layer.element.style.display, 'flex', 'Show even a hidden layer while measuring it');
            applied.push({ frameId: context.currentFrameId, layer, style: { ...layer.element.style } });
            layer.type = 'canvas';
            layer.canvas = { style: { display: 'block' } };
            delete layer.element;
            delete layer.gifGroupId;
            delete layer.gifGroupLinked;
            const item = { type: 'raster_image', frameId: context.currentFrameId, layerId: id };
            context.history.push(item);
            context.fullPlaybackRecord.push(item);
            context.redoStack = [];
        }
    };
    vm.runInNewContext(helpers, context, { filename: 'draw.html:gif-groups' });
    context.updateGifGroupControls = () => {};
    return { context, frames, applied, selected, alerts, errors };
}

test('finds only unfinished layers of the requested GIF and uses the current live layers', () => {
    const { context, frames } = createHarness();
    const liveLayer = transformLayer('gif-a');
    context.layers = [liveLayer];
    const members = context.getGifGroupMembers('gif-a');
    assert.equal(members.length, 3);
    assert.equal(members[0].layer, liveLayer);
    assert.notEqual(members[0].layer, frames[0].layers[1]);
    assert.deepEqual(members.map(member => member.frame.id), ['frame_0', 'frame_1', 'frame_3']);
    frames[1].layers[1].type = 'canvas';
    assert.equal(context.getGifGroupMembers('gif-a').length, 2);
    assert.equal(context.getGifGroupMembers(undefined).length, 0);
});

test('copies position, size and rotation across the same GIF without changing layer visibility or another GIF', () => {
    const { context, frames } = createHarness();
    const source = frames[0].layers[1];
    const otherGifBefore = { ...frames[2].layers[1].element.style };
    Object.assign(source.element.style, {
        left: '245px', top: '310px', width: '400px', height: '240px', transform: 'rotate(20deg)'
    });
    context.syncGifGroupTransform(source.element);
    for (const index of [1, 3]) {
        const layer = frames[index].layers[1];
        for (const key of ['left', 'top', 'width', 'height', 'transform']) {
            assert.equal(layer.element.style[key], source.element.style[key]);
        }
        assert.equal(layer.element.style.display, 'none');
        assert.equal(layer.element.style.opacity, '0.7');
    }
    assert.equal(frames[1].layers[1].visible, false);
    assert.deepEqual(frames[2].layers[1].element.style, otherGifBefore);
});

test('keeps individual GIF frames independent when linked editing is disabled', () => {
    const { context, frames } = createHarness();
    const source = frames[0].layers[1];
    source.gifGroupLinked = false;
    source.element.style.left = '999px';
    context.syncGifGroupTransform(source.element);
    assert.equal(frames[1].layers[1].element.style.left, '100px');
    assert.equal(frames[3].layers[1].element.style.left, '100px');
});

test('confirms the complete GIF across frames, saves histories and restores the original frame and layer', async () => {
    const { context, frames, applied, alerts, errors } = createHarness();
    context.selectFrame('frame_2');
    context.selectLayer('layer_1');
    const durations = frames.map(frame => frame.duration);
    const otherGif = frames[2].layers[1];
    await context.applyGifGroup('gif-a');

    assert.deepEqual(applied.map(item => item.frameId), ['frame_0', 'frame_1', 'frame_3']);
    assert.equal(context.currentFrameId, 'frame_2');
    assert.equal(context.activeLayerId, 'layer_1');
    assert.deepEqual(frames.map(frame => frame.duration), durations);
    assert.equal(otherGif.type, 'transform_box');
    assert.equal(otherGif.element.style.display, 'flex');
    for (const index of [0, 1, 3]) {
        const frame = frames[index];
        assert.equal(frame.layers[1].type, 'canvas');
        assert.equal(frame.history.length, 1);
        assert.equal(frame.fullPlaybackRecord.length, 1);
        assert.equal(frame.history[0].frameId, frame.id);
        assert.equal(frame.redoStack.length, 0);
        assert.equal(frame.layers[1].canvas.style.display, 'none');
    }
    assert.equal(frames[1].layers[1].visible, false);
    assert.equal(frames[2].history.length, 0);
    assert.equal(frames[2].redoStack.length, 1);
    assert.equal(context.isApplyingGifGroup, false);
    assert.equal(context.isRasterizing, false);
    assert.equal(alerts.length, 0);
    assert.equal(errors.length, 0);
});

test('preserves a hidden GIF layer after batch confirmation when restoring its frame', async () => {
    const { context, frames } = createHarness();
    context.selectFrame('frame_1');
    await context.applyGifGroup('gif-a');
    assert.equal(context.currentFrameId, 'frame_1');
    assert.equal(frames[1].layers[1].canvas.style.display, 'none');
    assert.equal(frames[1].layers[1].visible, false);
});

test('releases the batch lock and restores the original frame after a confirmation error', async () => {
    const { context, frames, applied, alerts, errors } = createHarness({ failFrame: 'frame_1' });
    context.selectFrame('frame_2');
    context.selectLayer('layer_1');
    await context.applyGifGroup('gif-a');
    assert.deepEqual(applied.map(item => item.frameId), ['frame_0']);
    assert.equal(frames[0].history.length, 1);
    assert.equal(frames[1].layers[1].type, 'transform_box');
    assert.equal(frames[3].layers[1].type, 'transform_box');
    assert.equal(context.currentFrameId, 'frame_2');
    assert.equal(context.activeLayerId, 'layer_1');
    assert.equal(context.isApplyingGifGroup, false);
    assert.equal(context.isRasterizing, false);
    assert.equal(alerts.length, 1);
    assert.equal(errors.length, 1);
});

test('does not start batch confirmation during another operation', async () => {
    for (const lock of ['isApplyingGifGroup', 'isRasterizing', 'isRedrawing', 'isPlaying']) {
        const { context, applied } = createHarness();
        context[lock] = true;
        await context.applyGifGroup('gif-a');
        assert.equal(applied.length, 0, lock);
    }
});
