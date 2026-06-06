export type SendMessageFn = (
  larkAppId: string,
  chatId: string,
  content: string,
  msgType?: string,
  uuid?: string,
  hookContext?: Record<string, unknown>,
) => Promise<string>;

export type ReplyMessageFn = (
  larkAppId: string,
  messageId: string,
  content: string,
  msgType?: string,
  replyInThread?: boolean,
  uuid?: string,
  hookContext?: Record<string, unknown>,
) => Promise<string>;

export type DispatchPrimaryDeps = {
  sendMessage: SendMessageFn;
  replyMessage: ReplyMessageFn;
};

export type DispatchPrimaryOptions = {
  appId: string;
  targetChatId: string;
  quoteTargetId: string | null | undefined;
  content: string;
  msgType: string;
  hookContext: Record<string, unknown>;
  MessageWithdrawnError: new (...args: any[]) => Error;
  dispatch: (content: string, msgType: string) => Promise<string>;
  onQuoteWithdrawn?: (messageId: string) => void;
};

export type DispatchPrimaryResult = {
  messageId: string;
  primaryQuotedId: string | null;
};

export async function dispatchPrimaryMessage(
  deps: DispatchPrimaryDeps,
  opts: DispatchPrimaryOptions,
): Promise<DispatchPrimaryResult> {
  if (!opts.quoteTargetId) {
    return {
      messageId: await opts.dispatch(opts.content, opts.msgType),
      primaryQuotedId: null,
    };
  }

  try {
    const messageId = await deps.replyMessage(
      opts.appId,
      opts.quoteTargetId,
      opts.content,
      opts.msgType,
      false,
      undefined,
      opts.hookContext,
    );
    return { messageId, primaryQuotedId: opts.quoteTargetId };
  } catch (err: any) {
    if (err instanceof opts.MessageWithdrawnError) {
      opts.onQuoteWithdrawn?.(opts.quoteTargetId);
      return {
        messageId: await deps.sendMessage(
          opts.appId,
          opts.targetChatId,
          opts.content,
          opts.msgType,
          undefined,
          opts.hookContext,
        ),
        primaryQuotedId: null,
      };
    }
    throw err;
  }
}
