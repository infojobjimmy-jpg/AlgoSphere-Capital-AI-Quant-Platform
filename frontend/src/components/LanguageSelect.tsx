import { Language, languages } from "../i18n";

export default function LanguageSelect({ value, onChange, className }: { value: Language; onChange: (value: Language) => void; className: string }) {
  const changeLanguage = (next: Language) => {
    localStorage.setItem("algosphere_lang", next);
    onChange(next);
    window.location.reload();
  };
  return (
    <select className={className} aria-label="Language" value={value} onChange={(event) => changeLanguage(event.target.value as Language)}>
      {languages.map((item) => <option key={item.code} value={item.code}>{item.label}</option>)}
    </select>
  );
}
