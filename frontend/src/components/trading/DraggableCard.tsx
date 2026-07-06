import { useCallback, useRef, useState, type ReactNode } from "react";

type Props = {
  title: string;
  children: ReactNode;
  className?: string;
  resizable?: boolean;
};

/** Draggable HUD card (header drag); optional vertical resize via CSS. */
export default function DraggableCard({ title, children, className, resizable }: Props) {
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const posRef = useRef(pos);
  posRef.current = pos;
  const origin = useRef({ px: 0, py: 0, x: 0, y: 0 });

  const onDown = useCallback((e: React.PointerEvent) => {
    e.currentTarget.setPointerCapture(e.pointerId);
    const { x, y } = posRef.current;
    origin.current = { px: e.clientX, py: e.clientY, x, y };
  }, []);

  const onMove = useCallback((e: React.PointerEvent) => {
    if (!e.currentTarget.hasPointerCapture(e.pointerId)) return;
    const o = origin.current;
    setPos({ x: o.x + (e.clientX - o.px), y: o.y + (e.clientY - o.py) });
  }, []);

  const onUp = useCallback((e: React.PointerEvent) => {
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
  }, []);

  return (
    <div
      className={`tl-float ${className ?? ""}`}
      style={{ transform: `translate(${pos.x}px, ${pos.y}px)` }}
    >
      <div className={`tl-card ${resizable ? "tl-resize-y" : ""}`}>
        <div
          className="tl-drag-h tl-card-h"
          role="presentation"
          onPointerDown={onDown}
          onPointerMove={onMove}
          onPointerUp={onUp}
          onPointerCancel={onUp}
        >
          {title}
        </div>
        {children}
      </div>
    </div>
  );
}
