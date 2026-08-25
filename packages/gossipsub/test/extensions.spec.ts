import { stop } from '@libp2p/interface'
import { multiaddr } from '@multiformats/multiaddr'
import { expect } from 'aegir/chai'
import { encode } from 'it-length-prefixed'
import { pEvent } from 'p-event'
import pWaitFor from 'p-wait-for'
import sinon from 'sinon'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'
import { GossipsubIDv13, GossipsubIDv12, GossipsubIDv11, GossipsubIDv10 } from '../src/constants.ts'
import { RPC } from '../src/message/rpc.ts'
import { OutboundStream } from '../src/stream.ts'
import { connectPubsubNodes, createComponentsArray } from './utils/create-pubsub.ts'
import type { GossipSub as GossipSubClass } from '../src/gossipsub.ts'
import type { GossipSubAndComponents } from './utils/create-pubsub.ts'
import type { Connection } from '@libp2p/interface'

type WithExtensionInternals = Partial<GossipSubClass> & {
  handleExtensions: (id: string, rpc: RPC, firstMessage: boolean, streamProtocol: string) => void
  peerExtensions: Map<string, RPC.ControlExtensions>
}

const extensionsRpc = (): RPC => ({
  subscriptions: [],
  messages: [],
  control: { ihave: [], iwant: [], graft: [], prune: [], idontwant: [], extensions: { testExtension: true } }
})

const emptyRpc = (): RPC => ({ subscriptions: [], messages: [] })
const emptyControl = (): RPC.ControlMessage => ({ ihave: [], iwant: [], graft: [], prune: [], idontwant: [] })

describe('extensions wire format', () => {
  it('should round-trip the extensions control message', () => {
    const rpc: RPC = {
      ...emptyRpc(),
      control: {
        ...emptyControl(),
        extensions: { testExtension: true }
      }
    }

    const decoded = RPC.decode(RPC.encode(rpc))
    expect(decoded.control?.extensions?.testExtension).to.be.true()

    const withoutBit: RPC = {
      ...emptyRpc(),
      control: { ...emptyControl(), extensions: {} }
    }
    const decodedWithoutBit = RPC.decode(RPC.encode(withoutBit))
    expect(decodedWithoutBit.control?.extensions).to.not.be.undefined()
    expect(decodedWithoutBit.control?.extensions?.testExtension).to.be.undefined()

    const withoutExtensions: RPC = { ...emptyRpc(), control: emptyControl() }
    expect(RPC.decode(RPC.encode(withoutExtensions)).control?.extensions).to.be.undefined()
  })

  it('should round-trip the top-level TestExtension message', () => {
    const rpc: RPC = { ...emptyRpc(), testExtension: {} }

    const decoded = RPC.decode(RPC.encode(rpc))
    expect(decoded.testExtension).to.not.be.undefined()

    expect(RPC.decode(RPC.encode(emptyRpc())).testExtension).to.be.undefined()
  })

  it('should lock the wire format of extension field numbers', () => {
    // ControlMessage.extensions = 6 (registry field number), ControlExtensions.testExtension
    // and RPC.testExtension = 6492434 (registry experimental range, >= 4 byte varint)
    const extensionsFixture = uint8ArrayFromString('1a0732059091e21801', 'base16')
    const rpc: RPC = {
      ...emptyRpc(),
      control: { ...emptyControl(), extensions: { testExtension: true } }
    }
    expect(RPC.encode(rpc)).to.equalBytes(extensionsFixture)
    expect(RPC.decode(extensionsFixture).control?.extensions?.testExtension).to.be.true()

    const testExtensionFixture = uint8ArrayFromString('9291e21800', 'base16')
    expect(RPC.encode({ ...emptyRpc(), testExtension: {} })).to.equalBytes(testExtensionFixture)
    expect(RPC.decode(testExtensionFixture).testExtension).to.not.be.undefined()
  })

  it('should decode extensions under the bounded decode limits', () => {
    // mirrors the limits shape used by GossipSub.decodeRpc
    const limits = {
      subscriptions: 5000,
      messages: 5000,
      control: {
        ihave: 5000,
        ihave$: { messageIDs: 5000 },
        iwant: 5000,
        iwant$: { messageIDs: 5000 },
        graft: 5000,
        prune: 5000,
        prune$: { peers: 16 },
        idontwant: 5000,
        idontwant$: { messageIDs: 512 }
      }
    }

    const rpc: RPC = {
      ...emptyRpc(),
      control: { ...emptyControl(), extensions: { testExtension: true } },
      testExtension: {}
    }
    const decoded = RPC.decode(RPC.encode(rpc), { limits })
    expect(decoded.control?.extensions?.testExtension).to.be.true()
    expect(decoded.testExtension).to.not.be.undefined()
  })

  it('should ignore unknown extension fields on decode', () => {
    // a subscription plus two unknown fields: a small unknown field number (9, varint)
    // and a large experimental-range one (7000000, length-delimited)
    const known = RPC.encode({
      ...emptyRpc(),
      subscriptions: [{ subscribe: true, topic: 'Z' }]
    })
    // field 9 varint: tag = 9 << 3 | 0 = 72 (0x48), value 1
    const unknownSmall = uint8ArrayFromString('4801', 'base16')
    // field 7000000 length-delimited: tag = 7000000 << 3 | 2 = 56000002 -> varint 82fcd91a, len 1, byte 0xff
    const unknownLarge = uint8ArrayFromString('82fcd91a01ff', 'base16')

    const frame = new Uint8Array(known.length + unknownSmall.length + unknownLarge.length)
    frame.set(known, 0)
    frame.set(unknownSmall, known.length)
    frame.set(unknownLarge, known.length + unknownSmall.length)

    const decoded = RPC.decode(frame)
    expect(decoded.subscriptions).to.have.length(1)
    expect(decoded.subscriptions[0].topic).to.equal('Z')
  })
})

describe('extensions handshake - sending', () => {
  let nodes: GossipSubAndComponents[]
  let pushSpy: sinon.SinonSpy

  // decode every RPC pushed per stream, in push order
  const pushedRpcsByStream = (): RPC[][] => {
    const byStream = new Map<unknown, RPC[]>()
    for (const call of pushSpy.getCalls()) {
      const rpcs = byStream.get(call.thisValue) ?? []
      rpcs.push(RPC.decode(call.args[0]))
      byStream.set(call.thisValue, rpcs)
    }
    return Array.from(byStream.values())
  }

  beforeEach(() => {
    nodes = []
    pushSpy = sinon.spy(OutboundStream.prototype, 'push')
  })

  afterEach(async () => {
    sinon.restore()
    await stop(...nodes.reduce<any[]>((acc, curr) => acc.concat(curr.pubsub, ...Object.entries(curr.components)), []))
  })

  it('should send extensions in the first message on the stream and never again', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true, allowPublishToZeroTopicPeers: true }
    })

    // connect before subscribing - the first message on the stream must carry the
    // extensions even when there are no subscriptions to send
    await connectPubsubNodes(nodes[0], nodes[1])

    // generate more traffic on the same streams
    const topic = 'Z'
    const subscriptionPromises = nodes.map(async (n) => pEvent(n.pubsub, 'subscription-change'))
    nodes.forEach((n) => { n.pubsub.subscribe(topic) })
    await Promise.all(subscriptionPromises)
    await Promise.all(nodes.map(async (n) => pEvent(n.pubsub, 'gossipsub:heartbeat')))
    await nodes[0].pubsub.publish(topic, uint8ArrayFromString('post-handshake traffic'))

    const streams = pushedRpcsByStream()
    expect(streams).to.have.length.greaterThan(0)

    for (const rpcs of streams) {
      // the first RPC on every stream carries our extensions
      expect(rpcs[0].control?.extensions?.testExtension, 'first RPC must carry extensions').to.be.true()
      // and no later RPC does
      for (const rpc of rpcs.slice(1)) {
        expect(rpc.control?.extensions, 'extensions must not be sent twice').to.be.undefined()
      }
    }
  })

  it('should not send extensions when we support none', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({ number: 2, connected: false })

    await connectPubsubNodes(nodes[0], nodes[1])
    const topic = 'Z'
    const subscriptionPromises = nodes.map(async (n) => pEvent(n.pubsub, 'subscription-change'))
    nodes.forEach((n) => { n.pubsub.subscribe(topic) })
    await Promise.all(subscriptionPromises)

    for (const rpcs of pushedRpcsByStream()) {
      for (const rpc of rpcs) {
        expect(rpc.control?.extensions).to.be.undefined()
      }
    }
  })

  it('should not send extensions on streams below v1.3', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true }
    })
    // the remote only speaks v1.2 - our extensions must stay off the wire
    nodes[1].pubsub.protocols = [GossipsubIDv12, GossipsubIDv11, GossipsubIDv10]

    await connectPubsubNodes(nodes[0], nodes[1])
    const topic = 'Z'
    const subscriptionPromises = nodes.map(async (n) => pEvent(n.pubsub, 'subscription-change'))
    nodes.forEach((n) => { n.pubsub.subscribe(topic) })
    await Promise.all(subscriptionPromises)

    for (const rpcs of pushedRpcsByStream()) {
      for (const rpc of rpcs) {
        expect(rpc.control?.extensions).to.be.undefined()
      }
    }
  })
})

describe('extensions handshake - receiving', () => {
  let nodes: GossipSubAndComponents[]

  beforeEach(() => {
    nodes = []
  })

  afterEach(async () => {
    sinon.restore()
    await stop(...nodes.reduce<any[]>((acc, curr) => acc.concat(curr.pubsub, ...Object.entries(curr.components)), []))
  })

  it('should record extensions advertised in the first message on the stream', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true }
    })
    const [nodeA, nodeB] = nodes
    const nodeAId = nodeA.components.peerId.toString()
    const nodeBId = nodeB.components.peerId.toString()

    await connectPubsubNodes(nodeA, nodeB)

    // both sides learn each other's extensions from the hello
    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    const pubsubB = nodeB.pubsub as unknown as WithExtensionInternals
    await pWaitFor(() => pubsubA.peerExtensions.get(nodeBId)?.testExtension === true)
    await pWaitFor(() => pubsubB.peerExtensions.get(nodeAId)?.testExtension === true)
  })

  it('should read peers that advertise nothing as supporting no extensions', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({ number: 2, connected: false })
    const [nodeA, nodeB] = nodes

    await connectPubsubNodes(nodeA, nodeB)
    const topic = 'Z'
    const subscriptionPromises = nodes.map(async (n) => pEvent(n.pubsub, 'subscription-change'))
    nodes.forEach((n) => { n.pubsub.subscribe(topic) })
    await Promise.all(subscriptionPromises)

    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    expect(pubsubA.peerExtensions.size).to.equal(0)
  })

  it('should ignore extensions that are not in the first message on the stream', async () => {
    nodes = await createComponentsArray({ number: 1, connected: false })
    const pubsub = nodes[0].pubsub as unknown as WithExtensionInternals

    pubsub.handleExtensions('peer-a', extensionsRpc(), false, GossipsubIDv13)
    expect(pubsub.peerExtensions.has('peer-a'), 'late extensions must be ignored').to.be.false()
  })

  it('should ignore extensions on streams below v1.3', async () => {
    nodes = await createComponentsArray({ number: 1, connected: false })
    const pubsub = nodes[0].pubsub as unknown as WithExtensionInternals

    pubsub.handleExtensions('peer-a', extensionsRpc(), true, GossipsubIDv12)
    expect(pubsub.peerExtensions.has('peer-a'), 'extensions below v1.3 must be ignored').to.be.false()

    // control: same message on a v1.3 stream is recorded
    pubsub.handleExtensions('peer-a', extensionsRpc(), true, GossipsubIDv13)
    expect(pubsub.peerExtensions.get('peer-a')?.testExtension).to.be.true()
  })

  it('should exchange TestExtension messages when both peers support it', async function () {
    this.timeout(10e4)
    const pushSpy = sinon.spy(OutboundStream.prototype, 'push')
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true }
    })
    const [nodeA, nodeB] = nodes
    const nodeAId = nodeA.components.peerId.toString()
    const nodeBId = nodeB.components.peerId.toString()

    await connectPubsubNodes(nodeA, nodeB)

    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    const pubsubB = nodeB.pubsub as unknown as WithExtensionInternals
    await pWaitFor(() => pubsubA.peerExtensions.get(nodeBId)?.testExtension === true)
    await pWaitFor(() => pubsubB.peerExtensions.get(nodeAId)?.testExtension === true)

    // each peer MUST send exactly one TestExtension message
    await pWaitFor(() => {
      const testExtensionRpcs = pushSpy.getCalls()
        .map((call) => RPC.decode(call.args[0]))
        .filter((rpc) => rpc.testExtension != null)
      return testExtensionRpcs.length === 2
    })

    // and never a second one for the same advertisement
    pubsubA.handleExtensions(nodeBId, extensionsRpc(), true, GossipsubIDv13)
    const testExtensionCount = pushSpy.getCalls()
      .map((call) => RPC.decode(call.args[0]))
      .filter((rpc) => rpc.testExtension != null)
      .length
    expect(testExtensionCount).to.equal(2)
  })

  it('should not send TestExtension when only one side supports it', async function () {
    this.timeout(10e4)
    const pushSpy = sinon.spy(OutboundStream.prototype, 'push')
    nodes = await createComponentsArray({ number: 2, connected: false })
    // only node A supports the test extension
    const withTestExtension = await createComponentsArray({
      number: 1,
      connected: false,
      init: { testExtension: true }
    })
    nodes.push(...withTestExtension)
    const nodeA = withTestExtension[0]
    const nodeB = nodes[0]
    const nodeAId = nodeA.components.peerId.toString()

    await connectPubsubNodes(nodeA, nodeB)
    const pubsubB = nodeB.pubsub as unknown as WithExtensionInternals
    await pWaitFor(() => pubsubB.peerExtensions.get(nodeAId)?.testExtension === true)

    const testExtensionRpcs = pushSpy.getCalls()
      .map((call) => RPC.decode(call.args[0]))
      .filter((rpc) => rpc.testExtension != null)
    expect(testExtensionRpcs).to.have.length(0)
  })

  it('should drop recorded extensions when the peer disconnects', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true }
    })
    const [nodeA, nodeB] = nodes
    const nodeBId = nodeB.components.peerId.toString()

    await connectPubsubNodes(nodeA, nodeB)
    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    await pWaitFor(() => pubsubA.peerExtensions.get(nodeBId)?.testExtension === true)

    await nodeA.components.connectionManager.closeConnections(nodeB.components.peerId)
    expect(pubsubA.peerExtensions.has(nodeBId), 'extensions must be dropped on disconnect').to.be.false()
  })

  it('should re-record extensions when the peer re-advertises on a new stream', async function () {
    this.timeout(10e4)
    const pushSpy = sinon.spy(OutboundStream.prototype, 'push')
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true }
    })
    const [nodeA, nodeB] = nodes
    const nodeBId = nodeB.components.peerId.toString()

    await connectPubsubNodes(nodeA, nodeB)
    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    await pWaitFor(() => pubsubA.peerExtensions.get(nodeBId)?.testExtension === true, { timeout: 5000 })

    // simulate B opening a replacement inbound stream to A whose first message
    // re-advertises B's extensions. Streams are message-event based - a bare
    // EventTarget with the stream fields the pipeline touches is enough
    const frame = encode.single(RPC.encode(extensionsRpc()))
    const fakeStream: any = new EventTarget()
    fakeStream.protocol = GossipsubIDv13
    fakeStream.close = async (): Promise<void> => {}
    fakeStream.abort = (): void => {}

    const fakeConnection = {
      remotePeer: nodeB.components.peerId,
      direction: 'inbound',
      remoteAddr: multiaddr('/memory/9999'),
      status: 'open'
    } as unknown as Connection

    const handleCall = nodeA.components.registrar.handle.getCalls().find((call) => call.args[0] === GossipsubIDv13)
    if (handleCall == null) {
      throw new Error('no v1.3 stream handler registered')
    }
    handleCall.args[1](fakeStream, fakeConnection)

    // the replacement stream reset the recorded extensions synchronously
    expect(pubsubA.peerExtensions.has(nodeBId), 'replacement must reset recorded extensions').to.be.false()

    // deliver the first message on the replacement stream
    const messageEvent: any = new Event('message')
    messageEvent.data = frame
    fakeStream.dispatchEvent(messageEvent)
    fakeStream.dispatchEvent(new Event('remoteCloseWrite'))

    // and the re-advertisement in its first message is recorded again
    await pWaitFor(() => pubsubA.peerExtensions.get(nodeBId)?.testExtension === true, { timeout: 5000 })

    // the handshake state was reset, so A answers the new advertisement with a
    // second TestExtension message (one each from the initial exchange, then one more)
    await pWaitFor(() => {
      const testExtensionRpcs = pushSpy.getCalls()
        .map((call) => RPC.decode(call.args[0]))
        .filter((rpc) => rpc.testExtension != null)
      return testExtensionRpcs.length >= 3
    }, { timeout: 5000 })
  })
})

describe('extensions version interop', () => {
  let nodes: GossipSubAndComponents[]

  afterEach(async () => {
    sinon.restore()
    await stop(...nodes.reduce<any[]>((acc, curr) => acc.concat(curr.pubsub, ...Object.entries(curr.components)), []))
  })

  it('should exchange messages between v1.3 and v1.2 peers', async function () {
    this.timeout(10e4)
    nodes = await createComponentsArray({
      number: 2,
      connected: false,
      init: { testExtension: true, allowPublishToZeroTopicPeers: true }
    })
    const [nodeA, nodeB] = nodes
    // B tops out at v1.2
    nodeB.pubsub.protocols = [GossipsubIDv12, GossipsubIDv11, GossipsubIDv10]

    const topic = 'Z'
    const subscriptionPromises = nodes.map(async (n) => pEvent(n.pubsub, 'subscription-change'))
    nodes.forEach((n) => { n.pubsub.subscribe(topic) })
    await connectPubsubNodes(nodeA, nodeB)
    await Promise.all(subscriptionPromises)
    await Promise.all(nodes.map(async (n) => pEvent(n.pubsub, 'gossipsub:heartbeat')))

    const received = pEvent<'gossipsub:message', CustomEvent>(nodeB.pubsub, 'gossipsub:message')
    await nodeA.pubsub.publish(topic, uint8ArrayFromString('interop message'))
    await received

    // no extensions were recorded on either side
    const pubsubA = nodeA.pubsub as unknown as WithExtensionInternals
    const pubsubB = nodeB.pubsub as unknown as WithExtensionInternals
    expect(pubsubA.peerExtensions.size).to.equal(0)
    expect(pubsubB.peerExtensions.size).to.equal(0)
  })
})
