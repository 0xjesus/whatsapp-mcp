"""Real OCR/PDF tests; optional dependencies installed from requirements.txt."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

@unittest.skipUnless(importlib.util.find_spec('rapidocr_onnxruntime'), 'OCR dependencies not installed')
class MediaTests(unittest.TestCase):
    def test_every_scanned_page_is_extracted(self):
        from PIL import Image, ImageDraw, ImageFont
        from tools.attachments.extract import extract
        with tempfile.TemporaryDirectory() as tmp:
            font = ImageFont.load_default(size=40)
            pages=[]
            for label in ['FIRST PAGE 1234','LAST PAGE 9876']:
                page=Image.new('RGB',(1200,300),'white')
                ImageDraw.Draw(page).text((30,100),label,font=font,fill='black')
                pages.append(page)
            path=Path(tmp)/'scan.pdf'
            pages[0].save(path,'PDF',save_all=True,append_images=pages[1:])
            result=extract(path)
            self.assertEqual(result['status'],'done')
            self.assertEqual(result['pages_processed'],2)
            self.assertIn('1234',result['text'])
            self.assertIn('9876',result['text'])

    @unittest.skipUnless(os.environ.get('WA_VISION_MODEL'), 'local vision model not configured')
    def test_real_local_visual_description_and_ocr(self):
        from PIL import Image, ImageDraw
        from tools.attachments.extract import extract
        from tools.attachments.vision import describe
        with tempfile.TemporaryDirectory() as tmp:
            page=Image.new('RGB',(512,512),'white')
            ImageDraw.Draw(page).rectangle((100,100,400,400),fill='red')
            path=Path(tmp)/'red.png'; page.save(path)
            result=extract(path,vision=describe)
            self.assertEqual(result['status'],'done')
            self.assertIn('red',result['text'].lower())
            self.assertIn('Descripción visual',result['text'])
