import { theme } from '@/theme';

describe('Theme Configuration', () => {
  describe('Colors', () => {
    it('should have deepBlue and blue color palettes defined', () => {
      expect(theme.colors?.deepBlue).toBeDefined();
      expect(theme.colors?.blue).toBeDefined();
    });

    it('should have 10 shades for deepBlue', () => {
      expect(theme.colors?.deepBlue?.length).toBe(10);
    });
  });

  describe('Shadows', () => {
    it('should define medium and extra-large shadows', () => {
      expect(theme.shadows?.md).toBeDefined();
      expect(theme.shadows?.xl).toBeDefined();
    });

    it('should have correct shadow value for md', () => {
      expect(theme.shadows?.md).toBe('1px 1px 3px rgba(0, 0, 0, .25)');
    });
  });

  describe('Typography', () => {
    it('should have a Roboto font family for headings', () => {
      expect(theme.headings?.fontFamily).toBe('Roboto, sans-serif');
    });

    it('should define font size for h1', () => {
      expect(theme.headings?.sizes?.h1?.fontSize).toBeDefined();
    });
  });
});
