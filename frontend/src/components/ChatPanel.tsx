import { useCallback, useEffect, useRef, useState } from 'react'
import { streamChat } from '../api'
import type { ChatMessage, ChatSource } from '../types'

interface Props {
  open: boolean
  onClose: () => void
}

export default function ChatPanel({ open, onClose }: Props) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const cancelRef = useRef<(() => void) | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const send = useCallback(() => {
    const query = input.trim()
    if (!query || busy) return
    setInput('')
    setBusy(true)

    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: query,
    }
    const assistantId = crypto.randomUUID()
    const assistantMsg: ChatMessage = {
      id: assistantId,
      role: 'assistant',
      content: '',
      sources: [],
      streaming: true,
    }
    setMessages((prev) => [...prev, userMsg, assistantMsg])

    cancelRef.current = streamChat(
      query,
      (sources: ChatSource[]) => {
        setMessages((prev) =>
          prev.map((m) => (m.id === assistantId ? { ...m, sources } : m))
        )
      },
      (token: string) => {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId ? { ...m, content: m.content + token } : m
          )
        )
      },
      () => {
        setMessages((prev) =>
          prev.map((m) => (m.id === assistantId ? { ...m, streaming: false } : m))
        )
        setBusy(false)
      },
      (err: Error) => {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: `Error: ${err.message}`, streaming: false }
              : m
          )
        )
        setBusy(false)
      },
    )
  }, [input, busy])

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex justify-end pointer-events-none">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40 pointer-events-auto"
        onClick={onClose}
      />
      {/* Panel */}
      <div className="relative w-[480px] h-full bg-slate-900 border-l border-slate-800
                      flex flex-col pointer-events-auto shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-800">
          <div>
            <span className="font-semibold text-slate-100">RiskLens AI</span>
            <p className="text-xs text-slate-500 mt-0.5">Powered by Claude</p>
          </div>
          <button onClick={onClose} className="btn-ghost text-lg leading-none">✕</button>
        </div>

        {/* Messages */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {messages.length === 0 && (
            <div className="text-center mt-16 space-y-3">
              <div className="text-4xl">✦</div>
              <p className="text-slate-400 text-sm">Ask about tables, lineage, quality, or FRTB concepts.</p>
              <div className="flex flex-col gap-2 mt-4">
                {[
                  'What tables contain VaR data?',
                  'Which assets have SLA breaches?',
                  'How does silver_trades get populated?',
                ].map((q) => (
                  <button
                    key={q}
                    onClick={() => { setInput(q) }}
                    className="text-left text-xs px-3 py-2 rounded-lg border border-slate-700
                               text-slate-400 hover:border-brand-500 hover:text-brand-400 transition-colors"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg) => (
            <div key={msg.id} className={`flex flex-col gap-2 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}>
              {msg.role === 'user' ? (
                <div className="max-w-[85%] bg-brand-600/20 border border-brand-600/30
                                rounded-2xl rounded-tr-sm px-4 py-2.5 text-sm text-slate-100">
                  {msg.content}
                </div>
              ) : (
                <div className="max-w-full space-y-2">
                  <div className="bg-slate-800 rounded-2xl rounded-tl-sm px-4 py-3 text-sm
                                  text-slate-200 whitespace-pre-wrap leading-relaxed">
                    {msg.streaming && msg.content === '' ? (
                      <span className="flex items-center gap-1 h-5">
                        <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ backgroundColor: '#818cf8', animationDelay: '0ms' }} />
                        <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ backgroundColor: '#818cf8', animationDelay: '150ms' }} />
                        <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ backgroundColor: '#818cf8', animationDelay: '300ms' }} />
                      </span>
                    ) : (
                      <>
                        {msg.content}
                        {msg.streaming && (
                          <span className="inline-block w-1.5 h-4 bg-brand-500 ml-1 animate-pulse align-middle" />
                        )}
                      </>
                    )}
                  </div>
                  {msg.sources && msg.sources.length > 0 && !msg.streaming && (
                    <SourceCards sources={msg.sources} />
                  )}
                </div>
              )}
            </div>
          ))}
          <div ref={bottomRef} />
        </div>

        {/* Input */}
        <div className="px-5 py-4 border-t border-slate-800">
          <div className="flex gap-2">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Ask about your data catalog…"
              rows={2}
              className="input flex-1 resize-none text-sm"
            />
            <button
              onClick={send}
              disabled={busy || !input.trim()}
              className="btn-primary self-end disabled:opacity-40 disabled:cursor-not-allowed px-4"
            >
              {busy ? '…' : '↑'}
            </button>
          </div>
          <p className="text-xs text-slate-600 mt-1.5">Enter to send · Shift+Enter for newline</p>
        </div>
      </div>
    </div>
  )
}

function SourceCards({ sources }: { sources: ChatSource[] }) {
  const typeColor: Record<string, string> = {
    asset_desc:   'text-blue-400',
    schema_doc:   'text-purple-400',
    pipeline_doc: 'text-teal-400',
  }
  return (
    <div className="space-y-1">
      <p className="text-xs text-slate-500 px-1">Sources</p>
      <div className="flex flex-wrap gap-1.5">
        {sources.map((s) => (
          <div
            key={s.chunk_id}
            className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-slate-800
                       border border-slate-700 text-xs"
          >
            <span className={typeColor[s.source_type] ?? 'text-slate-400'}>◆</span>
            <span className="text-slate-300 font-mono">{s.name}</span>
            <span className="text-slate-600">{s.domain}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
