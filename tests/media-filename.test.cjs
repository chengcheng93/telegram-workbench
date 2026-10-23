const test = require('node:test');
const assert = require('node:assert/strict');
const UI = require('../web/state.js');

function filename(disposition) {
  assert.equal(typeof UI.mediaFilename, 'function', '媒体文件名解析尚未实现');
  return UI.mediaFilename(disposition);
}

test('RFC5987 UTF-8 文件名保留中文、空格和扩展名', () => {
  assert.equal(filename("attachment; filename*=UTF-8''%E4%B8%AD%E6%96%87%20%E6%96%87%E4%BB%B6.pdf"), '中文 文件.pdf');
});

test('RFC5987 charset 大小写不敏感并接受语言字段', () => {
  assert.equal(filename("attachment; FILENAME*=uTf-8'zh-CN'%E6%8A%A5%E5%91%8A%20final.xlsx"), '报告 final.xlsx');
});

test('兼容引号内的 ASCII 文件名和分号', () => {
  assert.equal(filename('attachment; filename="quarter; final report.txt"'), 'quarter; final report.txt');
});

test('兼容不带引号的中文和空格文件名', () => {
  assert.equal(filename('attachment; filename=中文 文件.docx'), '中文 文件.docx');
});

test('同时存在两种文件名时不受参数顺序影响并优先 filename*', () => {
  for (const header of [
    "attachment; filename=plain.txt; filename*=UTF-8''%E4%B8%AD%E6%96%87.txt",
    "attachment; filename*=UTF-8''%E4%B8%AD%E6%96%87.txt; filename=plain.txt"
  ]) assert.equal(filename(header), '中文.txt');
});

test('RFC5987 解码失败时回退原文件名且不抛错', () => {
  assert.equal(filename("attachment; filename=original file.txt; filename*=UTF-8''%E0%A4%A"), 'original file.txt');
  assert.equal(filename("attachment; filename*=UTF-8''bad%ZZ.txt"), 'telegram-attachment');
});

test('不支持的 charset 和不完整扩展参数安全回退原文件名', () => {
  assert.equal(filename("attachment; filename=fallback.txt; filename*=ISO-8859-1''caf%E9.txt"), 'fallback.txt');
  assert.equal(filename('attachment; filename="fallback.txt"; filename*=invalid'), 'fallback.txt');
});

test('删除解码文件名和原文件名中的路径分隔及控制字符', () => {
  assert.equal(filename("attachment; filename*=UTF-8''..%2Ffolder%5C%E4%B8%AD%E6%96%87%00%1F%7F.txt"), '..folder中文.txt');
  assert.equal(filename('attachment; filename="../folder\\file\u0000\u001f\u007f.txt"'), '..folderfile.txt');
});

test('空文件名、仅分隔符及点目录都使用安全默认名', () => {
  for (const header of [
    'attachment; filename=""',
    'attachment; filename=" /\\ "',
    'attachment; filename="."',
    'attachment; filename=".."',
    "attachment; filename*=UTF-8''%2F%5C%00%7F",
    "attachment; filename*=UTF-8''.."
  ]) assert.equal(filename(header), 'telegram-attachment');
});

test('无 Content-Disposition 或无文件名时使用安全默认名', () => {
  for (const header of [undefined, null, '', 'inline', 'attachment; size=42']) {
    assert.equal(filename(header), 'telegram-attachment');
  }
});
