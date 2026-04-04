type Props = {
  title: string;
  description: string;
};

export const EmptyState = ({ title, description }: Props) => (
  <div className="rounded-lg border border-dashed border-white/20 p-6 text-center">
    <p className="font-semibold">{title}</p>
    <p className="mt-1 text-sm text-foreground/70">{description}</p>
  </div>
);
