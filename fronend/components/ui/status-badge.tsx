import { cn } from "@/lib/utils";

type Props = {
  status: "queued" | "running" | "completed" | "failed" | "cancelled" | "waiting";
};

const colorMap: Record<Props["status"], string> = {
  queued: "bg-muted text-foreground",
  running: "bg-primary/20 text-primary",
  completed: "bg-success/20 text-success",
  failed: "bg-danger/20 text-danger",
  cancelled: "bg-warning/20 text-warning",
  waiting: "bg-muted text-foreground"
};

export const StatusBadge = ({ status }: Props) => (
  <span className={cn("inline-flex rounded-full px-2 py-1 text-xs font-medium", colorMap[status])}>{status}</span>
);
