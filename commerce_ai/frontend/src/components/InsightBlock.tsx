interface InsightBlockProps {
  text: string
  source?: string
}

export default function InsightBlock({ text, source }: InsightBlockProps) {
  return (
    <div className="bg-gray-50 border-l-[3px] border-gray-300 p-4 rounded-r-lg">
      <div className="flex items-center gap-1.5 mb-2">
        <span className="text-gray-500 text-xs">◆</span>
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">
          AI Insight
        </span>
      </div>
      <p className="text-sm text-gray-900 leading-relaxed">{text}</p>
      {source && <p className="text-xs text-gray-500 mt-2">{source}</p>}
    </div>
  )
}

export type { InsightBlockProps }
