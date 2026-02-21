import React from 'react';
import '../styles/insider-activity.css';

export function InsiderActivity({ data, isStreaming = false }: { data: any, isStreaming?: boolean }) {
    if (!data || !data.transactions || data.transactions.length === 0) return null;

    const netClass = data.net_direction === 'NET BUYING' ? 'text-green' : data.net_direction === 'NET SELLING' ? 'text-red' : 'text-gray';

    return (
        <div className={`terminal-section insider-activity ${isStreaming ? 'streaming' : ''}`}>
            <div className="section-header flex-between">
                <span>INSIDER ACTIVITY</span>
                <span className={`net-badge ${netClass}`}>[{data.net_direction}]</span>
            </div>
            <table className="terminal-table mt-2">
                <thead>
                    <tr>
                        <th>DATE</th>
                        <th>NAME</th>
                        <th>TYPE</th>
                        <th className="text-right">SHARES</th>
                    </tr>
                </thead>
                <tbody>
                    {data.transactions.slice(0, 5).map((t: any, i: number) => (
                        <tr key={i}>
                            <td className="text-gray">{t.date}</td>
                            <td className="truncate" title={`${t.name} - ${t.title}`}>{t.name.split(' ')[0]} {t.name.split(' ').slice(-1)}</td>
                            <td className={t.type === 'BUY' ? 'text-green' : t.type === 'SELL' ? 'text-red' : ''}>{t.type}</td>
                            <td className="text-right">{Math.abs(t.shares).toLocaleString()}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}
