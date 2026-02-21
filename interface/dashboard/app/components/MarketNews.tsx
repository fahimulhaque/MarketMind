import React from 'react';
import '../styles/market-news.css';

export function MarketNews({ data, isStreaming = false }: { data: any, isStreaming?: boolean }) {
    if (!data || !data.articles || data.articles.length === 0) return null;

    return (
        <div className={`terminal-section market-news mt-4 ${isStreaming ? 'streaming' : ''}`}>
            <div className="section-header">MARKET NEWS</div>
            <ul className="news-list mt-2">
                {data.articles.slice(0, 5).map((article: any, i: number) => {
                    let date = '[NO DATE]';
                    if (article.timestamp) {
                        try {
                            const d = typeof article.timestamp === 'number'
                                ? new Date(article.timestamp * 1000)
                                : new Date(article.timestamp);
                            date = d.toISOString().split('T')[0];
                        } catch (e) {
                            date = String(article.timestamp).slice(0, 10);
                        }
                    }
                    return (
                        <li key={i} className="news-item flex gap-2">
                            <span className="text-gray shrink-0">[{date}]</span>
                            <span className="text-gray shrink-0">{article.publisher}:</span>
                            <a href={article.link} target="_blank" rel="noreferrer" className="text-white hover:underline truncate">
                                {article.title}
                            </a>
                        </li>
                    );
                })}
            </ul>
        </div>
    );
}
