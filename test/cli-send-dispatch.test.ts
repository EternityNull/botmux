import { describe, expect, it, vi } from 'vitest';

import { dispatchPrimaryMessage } from '../src/cli/send-dispatch.js';

class MessageWithdrawnError extends Error {}

describe('dispatchPrimaryMessage hook context wiring', () => {
  const baseOptions = {
    appId: 'cli_app',
    targetChatId: 'oc_chat',
    hookContext: {
      sessionId: 'sid_1',
      chatId: 'oc_chat',
      rootMessageId: 'om_root',
      title: 'Hook Context',
    },
    MessageWithdrawnError,
  };

  it('passes hookContext when quote reply succeeds', async () => {
    const replyMessage = vi.fn(async () => 'om_reply');
    const sendMessage = vi.fn(async () => 'om_send');

    const result = await dispatchPrimaryMessage(
      { replyMessage, sendMessage },
      {
        ...baseOptions,
        quoteTargetId: 'om_quote',
        dispatch: vi.fn(async () => 'om_dispatch'),
        content: '{"schema":"2.0"}',
        msgType: 'interactive',
      },
    );

    expect(result).toEqual({ messageId: 'om_reply', primaryQuotedId: 'om_quote' });
    expect(replyMessage).toHaveBeenCalledWith(
      'cli_app',
      'om_quote',
      '{"schema":"2.0"}',
      'interactive',
      false,
      undefined,
      baseOptions.hookContext,
    );
    expect(sendMessage).not.toHaveBeenCalled();
  });

  it('passes hookContext when withdrawn quote falls back to plain send', async () => {
    const replyMessage = vi.fn(async () => {
      throw new MessageWithdrawnError('withdrawn');
    });
    const sendMessage = vi.fn(async () => 'om_send');

    const result = await dispatchPrimaryMessage(
      { replyMessage, sendMessage },
      {
        ...baseOptions,
        quoteTargetId: 'om_quote',
        dispatch: vi.fn(async () => 'om_dispatch'),
        content: '{"zh_cn":{"content":[]}}',
        msgType: 'post',
      },
    );

    expect(result).toEqual({ messageId: 'om_send', primaryQuotedId: null });
    expect(sendMessage).toHaveBeenCalledWith(
      'cli_app',
      'oc_chat',
      '{"zh_cn":{"content":[]}}',
      'post',
      undefined,
      baseOptions.hookContext,
    );
  });
});
