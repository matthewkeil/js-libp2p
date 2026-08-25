import { expect } from 'aegir/chai'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'
import { RPC } from '../src/message/rpc.ts'

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
